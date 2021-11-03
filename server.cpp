

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdint.h>

#include <nng/nng.h>
#include <nng/protocol/reqrep0/rep.h>
#include <nng/supplemental/util/platform.h>

#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/signal_set.hpp>
#include <asio/write.hpp>
#include "asio/system_timer.hpp"
#include "asio/detached.hpp"
#include "asio/windows/stream_handle.hpp"
#include "asio/ssl.hpp"
#include "asio/co_spawn.hpp"
#include <cppcoro/schedule_on.hpp>
#include <cppcoro/single_consumer_event.hpp>
#include "cppcoro/single_consumer_async_auto_reset_event.hpp"
#include <cppcoro/sync_wait.hpp>
#include <cppcoro/task.hpp>
#include <cppcoro/when_all.hpp>
#include "cppcoro/async_scope.hpp"
#include <cstdlib>
#include <iostream>
#include <memory>
#include <utility>
#include <thread>
#include <memory>
#include <set>
#include <map>

#include <taskflow/taskflow.hpp>

using std::cout;
using std::endl;


tf::Executor _ex;

using cppcoro::schedule_on;
using cppcoro::single_consumer_event;
using cppcoro::sync_wait;
using cppcoro::task;
using cppcoro::when_all;

using asio::use_awaitable;
using asio::detached;
using asio::awaitable;
using asio::co_spawn;

using namespace std::chrono_literals;

#include <Windows.h>

#ifndef PARALLEL
#define PARALLEL 8*2
#endif

long _g = 0;
enum State { INIT, RECV, WAIT, SEND } ;
// The server keeps a list of work items, sorted by expiration time,
// so that we can use this to set the timeout to the correct value for
// use in poll.
struct work {
	State state;
	nng_aio* aio;
	nng_msg* msg;
	nng_ctx  ctx;
};

void
fatal(const char* func, int rv)
{
	fprintf(stderr, "%s: %s\n", func, nng_strerror(rv));
	exit(1);
}

cppcoro::async_scope _sc;

cppcoro::task<void> taskx(struct work* work, nng_msg* msg)
{
	work->msg = msg;
	work->state = WAIT;
	cppcoro::single_consumer_async_auto_reset_event event;
	_ex.async([&]() {
		event.set();
		});
	co_await event;
	// We could add more data to the message here.
	nng_aio_set_msg(work->aio, work->msg);
	work->msg = NULL;
	work->state = SEND;
	nng_ctx_send(work->ctx, work->aio);
	co_return;
}

void
server_cb(void* arg)
{
	struct work* work = (struct work* )arg;
	nng_msg* msg;
	int          rv;
	uint32_t     when;

	switch (work->state) {
	case INIT:
		work->state = RECV;
		nng_ctx_recv(work->ctx, work->aio);
		break;
	case RECV:
		if ((rv = nng_aio_result(work->aio)) != 0) {
			fatal("nng_ctx_recv", rv);
		}
		msg = nng_aio_get_msg(work->aio);
		if ((rv = nng_msg_trim_u32(msg, &when)) != 0) {
			// bad message, just ignore it.
			nng_msg_free(msg);
			nng_ctx_recv(work->ctx, work->aio);
			return;
		}
		_sc.spawn(taskx(work, msg));
		InterlockedAdd(&_g, 1);
		if (_g % 100000 == 0)
		{
			time_t now;
			time(&now);
			struct tm* info = localtime(&now);
			printf("%s", asctime(info));
			printf("count:[%d]\n", _g);
		}
		break;
	case SEND:
		if ((rv = nng_aio_result(work->aio)) != 0) {
			nng_msg_free(work->msg);
			fatal("nng_ctx_send", rv);
		}
		work->state = RECV;
		nng_ctx_recv(work->ctx, work->aio);
		break;
	default:
		fatal("bad state!", NNG_ESTATE);
		break;
	}
}

struct work*
	alloc_work(nng_socket sock)
{
	struct work* w;
	int          rv;

	if ((w = (struct work* )nng_alloc(sizeof(*w))) == NULL) {
		fatal("nng_alloc", NNG_ENOMEM);
	}
	if ((rv = nng_aio_alloc(&w->aio, server_cb, w)) != 0) {
		fatal("nng_aio_alloc", rv);
	}
	if ((rv = nng_ctx_open(&w->ctx, sock)) != 0) {
		fatal("nng_ctx_open", rv);
	}
	w->state = INIT;
	return (w);
}

// The server runs forever.
int
server(const char* url)
{
	nng_socket   sock;
	struct work* works[PARALLEL];
	int          rv;
	int          i;

	/*  Create the socket. */
	rv = nng_rep0_open(&sock);
	if (rv != 0) {
		fatal("nng_rep0_open", rv);
	}

	for (i = 0; i < PARALLEL; i++) {
		works[i] = alloc_work(sock);
	}

	if ((rv = nng_listen(sock, url, NULL, 0)) != 0) {
		fatal("nng_listen", rv);
	}

	for (i = 0; i < PARALLEL; i++) {
		server_cb(works[i]); // this starts them going (INIT state)
	}

	for (;;) {
		nng_msleep(3600000); // neither pause() nor sleep() portable
	}
}

int
main(int argc, char** argv)
{
	int rc;

	if (argc != 2) {
		fprintf(stderr, "Usage: %s <url>\n", argv[0]);
		exit(EXIT_FAILURE);
	}
	rc = server(argv[1]);
	exit(rc == 0 ? EXIT_SUCCESS : EXIT_FAILURE);
}
