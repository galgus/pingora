use super::*;
use crate::proxy_cache::{range_filter::RangeBodyFilter, ServeFromCache};
use crate::proxy_common::*;
use crate::proxy_h1::send_body_to_buffer;
use bytes::{BufMut, BytesMut};

impl<SV> HttpProxy<SV> {
    pub(crate) async fn proxy_handle_downstream_mid(
        &self,
        session: &mut Session,
        tx: mpsc::Sender<HttpTask>,
        mut rx: mpsc::Receiver<HttpTask>,
        ctx: &mut SV::CTX,
    ) -> Result<()>
    where
        SV: ProxyHttp + Send + Sync,
        SV::CTX: Send + Sync,
    {
        let content_length = session
            .req_header()
            .headers
            .get(http::header::CONTENT_LENGTH)
            .map_or(1000, |v| {
                str::from_utf8(v.as_bytes())
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(1000)
            });
        let mut buf = BytesMut::with_capacity(content_length);
        let mut downstream_state = DownstreamStateMachine::new(session.as_mut().is_body_done());

        let buffer = session.as_ref().get_retry_buffer();

        // retry, send buffer if it exists or body empty
        if buffer.is_some() || session.as_mut().is_body_empty() {
            let send_permit = tx
                .reserve()
                .await
                .or_err(InternalError, "reserving body pipe")?;
            self.send_body_to_pipe(
                session,
                buffer,
                downstream_state.is_done(),
                send_permit,
                ctx,
            )
            .await?;
        }

        let mut response_state = ResponseStateMachine::new();

        // these two below can be wrapped into an internal ctx
        // use cache when upstream revalidates (or TODO: error)
        let mut serve_from_cache = proxy_cache::ServeFromCache::new();
        let mut range_body_filter = proxy_cache::range_filter::RangeBodyFilter::new();

        /* duplex mode without caching
         * Read body from downstream while reading response from upstream
         * If response is done, only read body from downstream
         * If request is done, read response from upstream while idling downstream (to close quickly)
         * If both are done, quit the loop
         *
         * With caching + but without partial read support
         * Similar to above, cache admission write happen when the data is write to downstream
         *
         * With caching + partial read support
         * A. Read upstream response and write to cache
         * B. Read data from cache and send to downstream
         * If B fails (usually downstream close), continue A.
         * If A fails, exit with error.
         * If both are done, quit the loop
         * Usually there is no request body to read for cacheable request
         */
        while !downstream_state.is_done() {
            // only try to send to pipe if there is capacity to avoid deadlock
            // Otherwise deadlock could happen if both upstream and downstream are blocked
            // on sending to their corresponding pipes which are both full.
            if downstream_state.can_poll() {
                let body = session
                    .downstream_session
                    .read_body_or_idle(downstream_state.is_done())
                    .await;

                debug!("downstream event");
                let body = match body {
                    Ok(b) => b,
                    Err(e) => {
                        if serve_from_cache.is_miss() {
                            // ignore downstream error so that upstream can continue write cache
                            downstream_state.to_errored();
                            warn!(
                                "Downstream Error ignored during caching: {}, {}",
                                e,
                                self.inner.request_summary(session, ctx)
                            );
                            continue;
                        } else {
                            return Err(e.into_down());
                        }
                    }
                };
                // If the request is websocket, `None` body means the request is closed.
                // Set the response to be done as well so that the request completes normally.
                if body.is_none() && session.is_upgrade_req() {
                    response_state.maybe_set_upstream_done(true);
                }
                // TODO: consider just drain this if serve_from_cache is set
                let request_done =
                    send_body_to_buffer(body, session.is_body_done(), &mut buf /*, &mut pos*/);
                downstream_state.maybe_finished(request_done);
            } else {
                break;
            }
        }

        let mut buf = Some(buf.freeze());
        match self
            .inner
            .upstream_request_body_filter(session, &mut buf, ctx)
            .await
        {
            Ok(_) => { /* continue */ }
            Err(e) => return Err(e),
        }

        loop {
            // reserve tx capacity ahead to avoid deadlock, see below
            let send_permit = tx
                .try_reserve()
                .or_err(InternalError, "try_reserve() body pipe for upstream");

            match send_permit {
                Ok(send_permit) => {
                    if let Err(e) = self
                        .send_body_to_pipe(session, buf.take(), true, send_permit, ctx)
                        .await
                    {
                        warn!("send_body_to_pipe: {e:?}");
                    }
                    break;
                }
                Err(ref _e) => {
                    debug!("waiting for permit {send_permit:?}");
                    if let Err(e) = tx.reserve().await {
                        warn!("reserve: {e:?}");
                        break;
                    }
                    /* No permit, wait on more capacity to avoid starving.
                     * Otherwise this select only blocks on rx, which might send no data
                     * before the entire body is uploaded.
                     * once more capacity arrives we just loop back
                     */
                }
            }
        }

        let mut downstream_state = DownstreamStateMachine::new(false);

        while !downstream_state.is_done() || !response_state.is_done() {
            tokio::select! {
                task = rx.recv(), if !response_state.upstream_done() => {
                    debug!("upstream event: {:?}", task);
                    if let Some(t) = task {
                        if serve_from_cache.should_discard_upstream() {
                            // just drain, do we need to do anything else?
                           continue;
                        }
                        // pull as many tasks as we can
                        let mut tasks = Vec::with_capacity(TASK_BUFFER_SIZE);
                        tasks.push(t);
                        while let Some(maybe_task) = rx.recv().now_or_never() {
                            debug!("upstream event now: {:?}", maybe_task);
                            if let Some(t) = maybe_task {
                                tasks.push(t);
                            } else {
                                break; // upstream closed
                            }
                        }

                        /* run filters before sending to downstream */
                        let mut filtered_tasks = Vec::with_capacity(TASK_BUFFER_SIZE);
                        for mut t in tasks {
                            if self.revalidate_or_stale(session, &mut t, ctx).await {
                                serve_from_cache.enable();
                                response_state.enable_cached_response();
                                // skip downstream filtering entirely as the 304 will not be sent
                                break;
                            }
                            session.upstream_compression.response_filter(&mut t);
                            let task = self.h1_response_filter(session, t, ctx,
                                &mut serve_from_cache,
                                &mut range_body_filter, false).await?;
                            if serve_from_cache.is_miss_header() {
                                response_state.enable_cached_response();
                            }
                            // check error and abort
                            // otherwise the error is surfaced via write_response_tasks()
                            if !serve_from_cache.should_send_to_downstream() {
                                if let HttpTask::Failed(e) = task {
                                    return Err(e);
                                }
                            }
                            filtered_tasks.push(task);
                        }

                        if !serve_from_cache.should_send_to_downstream() {
                            // TODO: need to derive response_done from filtered_tasks in case downstream failed already
                            continue;
                        }

                        // set to downstream
                        let response_done = session.write_response_tasks(filtered_tasks).await?;
                        response_state.maybe_set_upstream_done(response_done);
                        // unsuccessful upgrade response may force the request done
                        downstream_state.maybe_finished(session.is_body_done()); // JADA: Que hacemos con esto?
                    } else {
                        debug!("empty upstream event");
                        response_state.maybe_set_upstream_done(true);
                    }
                },

                task = serve_from_cache.next_http_task(&mut session.cache),
                    if !response_state.cached_done() && !downstream_state.is_errored() && serve_from_cache.is_on() => {

                    let task = self.h1_response_filter(session, task?, ctx,
                        &mut serve_from_cache,
                        &mut range_body_filter, true).await?;
                    debug!("serve_from_cache task {task:?}");

                    match session.write_response_tasks(vec![task]).await {
                        Ok(b) => response_state.maybe_set_cache_done(b),
                        Err(e) => if serve_from_cache.is_miss() {
                            // give up writing to downstream but wait for upstream cache write to finish
                            downstream_state.to_errored();
                            response_state.maybe_set_cache_done(true);
                            warn!(
                                "Downstream Error ignored during caching: {}, {}",
                                e,
                                self.inner.request_summary(session, ctx)
                            );
                            continue;
                        } else {
                            return Err(e);
                        }
                    }
                    if response_state.cached_done() {
                        if let Err(e) = session.cache.finish_hit_handler().await {
                            warn!("Error during finish_hit_handler: {}", e);
                        }
                    }
                },

                else => {
                    break;
                },
            }
        }

        match session.as_mut().finish_body().await {
            Ok(_) => {
                debug!("finished sending body to downstream");
            }
            Err(e) => {
                error!("Error finish sending body to downstream: {}", e);
                // TODO: don't do downstream keepalive
            }
        }
        Ok(())
    }
}
