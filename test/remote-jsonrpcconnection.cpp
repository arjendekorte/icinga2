/* Icinga 2 | (c) 2025 Icinga GmbH | GPLv2+ */

#include <BoostTestTargetConfig.h>

#include "base/json.hpp"
#include "remote/apifunction.hpp"
#include "remote/jsonrpc.hpp"
#include "remote/jsonrpcconnection.hpp"
#include "test/base-testloggerfixture.hpp"
#include "test/base-tlsstream-fixture.hpp"
#include "test/icingaapplication-fixture.hpp"
#include "test/test-ctest.hpp"
#include "test/test-timedasserts.hpp"
#include <utility>

using namespace icinga;

class JsonRpcConnectionFixture : public TlsStreamFixture, public TestLoggerFixture, IcingaApplicationFixture
{
public:
	JsonRpcConnectionFixture()
	{
		ScriptGlobal::Set("NodeName", "server");
		ApiListener::Ptr listener = new ApiListener;
		listener->OnConfigLoaded();
	}

	JsonRpcConnection::Ptr ConnectEndpoint(
		const Shared<AsioTlsStream>::Ptr& stream,
		const String& identity,
		bool authenticated = true
	)
	{
		Zone::Ptr zone = new Zone;
		zone->SetName(identity);
		zone->Register();

		Endpoint::Ptr endpoint = new Endpoint;
		endpoint->SetName(identity);
		endpoint->SetZoneName(identity);
		endpoint->Register();

		StoppableWaitGroup::Ptr wg = new StoppableWaitGroup;
		JsonRpcConnection::Ptr conn = new JsonRpcConnection(wg, identity, authenticated, stream, RoleClient);
		conn->Start();

		endpoint->AddClient(conn);
		m_Connections[conn] = std::move(wg);

		return conn;
	}

	using ConnectionPair = std::pair<JsonRpcConnection::Ptr, JsonRpcConnection::Ptr>;

	ConnectionPair ConnectEndpointPair(const String& nameA, const String& nameB, bool authenticated = true)
	{
		auto aToB = ConnectEndpoint(client, nameB, authenticated);
		auto bToA = ConnectEndpoint(server, nameA, authenticated);
		return std::make_pair(std::move(aToB), std::move(bToA));
	}

	void JoinWaitgroup(const JsonRpcConnection::Ptr& conn) { m_Connections[conn]->Join(); }

private:
	std::map<JsonRpcConnection::Ptr, StoppableWaitGroup::Ptr> m_Connections;
};

class TestApiHandler
{
public:
	struct Message
	{
		MessageOrigin::Ptr origin;
		Dictionary::Ptr params;
	};

	using TestFn = std::function<Value(Message)>;

	static void RegisterTestFn(std::string handle, TestFn fn) { m_TestFns[std::move(handle)] = std::move(fn); }

	static Value TestApiFunction(const MessageOrigin::Ptr& origin, const Dictionary::Ptr& params)
	{
		auto it = m_TestFns.find(String(params->Get("test")).GetData());
		if (it != m_TestFns.end()) {
			return it->second(Message{origin, params});
		}
		return Empty;
	}

private:
	static inline std::unordered_map<std::string, TestFn> m_TestFns;
};

REGISTER_APIFUNCTION(Test, test, &TestApiHandler::TestApiFunction);

// clang-format off
BOOST_FIXTURE_TEST_SUITE(remote_jsonrpcconnection, JsonRpcConnectionFixture,
	*CTestProperties("FIXTURES_REQUIRED ssl_certs")
	*boost::unit_test::label("cluster"))
// clang-format on

BOOST_AUTO_TEST_CASE(construction)
{
	auto test = ConnectEndpoint(server, "test");
	BOOST_REQUIRE_EQUAL(test->GetEndpoint()->GetName(), "test");
	BOOST_REQUIRE_EQUAL(test->GetIdentity(), "test");
	BOOST_REQUIRE_CLOSE(test->GetTimestamp(), Utility::GetTime(), 1);
	BOOST_REQUIRE(test->IsAuthenticated());
}

BOOST_AUTO_TEST_CASE(sendmessage)
{
	auto [aToB, bToA] = ConnectEndpointPair("a", "b");

	// clang-format off
	Dictionary::Ptr message = new Dictionary({
		{ "jsonrpc", "2.0" },
		{ "method", "test::Test" },
		{ "params", new Dictionary{{"test", "test"}} }
	});
	// clang-format on

	std::promise<TestApiHandler::Message> msgPromise;
	auto msgFuture = msgPromise.get_future();
	TestApiHandler::RegisterTestFn("test", [&](TestApiHandler::Message msg) {
		msgPromise.set_value(std::move(msg));
		return Empty;
	});

	// Test sending a regular message from a->b
	aToB->SendMessage(message);

	TestApiHandler::Message msg;
	CHECK_DONE_WITHIN(msg = msgFuture.get(), 1s);
	BOOST_REQUIRE_EQUAL(msg.origin->FromClient, bToA);
	BOOST_REQUIRE_EQUAL(msg.params->Get("test"), "test");

	aToB->Disconnect();
	CHECK_WITHIN(!client->lowest_layer().is_open() && !server->lowest_layer().is_open(), 1s);

	// Sending messages after disconnect should result in an exception
	BOOST_REQUIRE_THROW(aToB->SendMessage(message), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(sendmessageraw)
{
	auto [aToB, bToA] = ConnectEndpointPair("a", "b");

	// clang-format off
	auto message = JsonEncode(new Dictionary{{
		{ "jsonrpc", "2.0" },
		{ "method", "test::Test" },
		{ "params", new Dictionary{{"test", "test"}} }
	}});
	// clang-format on

	std::promise<TestApiHandler::Message> msgPromise;
	auto msgFuture = msgPromise.get_future();
	TestApiHandler::RegisterTestFn("test", [&](TestApiHandler::Message msg) {
		msgPromise.set_value(std::move(msg));
		return Empty;
	});

	// Test sending a raw message from b->a
	bToA->SendRawMessage(message);

	TestApiHandler::Message msg;
	CHECK_DONE_WITHIN(msg = msgFuture.get(), 1s);
	BOOST_REQUIRE_EQUAL(msg.origin->FromClient, aToB);
	BOOST_REQUIRE_EQUAL(msg.params->Get("test"), "test");

	aToB->Disconnect();
	CHECK_WITHIN(!client->lowest_layer().is_open() && !server->lowest_layer().is_open(), 1s);

	// Sending messages after disconnect should result in an exception
	BOOST_REQUIRE_THROW(bToA->SendRawMessage(message), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(old_message)
{
	auto conn = ConnectEndpoint(server, "test");
	BOOST_REQUIRE_EQUAL(conn->GetEndpoint()->GetRemoteLogPosition(), Timestamp{});

	auto ts = Utility::GetTime();
	// clang-format off
	Dictionary::Ptr message = new Dictionary{{
		{ "jsonrpc", "2.0" },
		{ "method", "test::Test" },
		{ "ts", ts },
		{ "params", new Dictionary{{"test", "test"}} }
	}};
	// clang-format on

	BOOST_REQUIRE_NO_THROW(JsonRpc::SendMessage(client, message));
	client->flush();

	CHECK_LOG_MESSAGE("Received 'test::Test' message from identity 'test'.", 1s);
	BOOST_CHECK_EQUAL(conn->GetEndpoint()->GetRemoteLogPosition(), ts);
	testLogger->Clear();

	message->Set("ts", Timestamp{});
	BOOST_REQUIRE_NO_THROW(JsonRpc::SendMessage(client, message));
	client->flush();

	CHECK_LOG_MESSAGE("Processed JSON-RPC 'test::Test' message for identity 'test'.*", 1s);
	CHECK_NO_LOG_MESSAGE("Received 'test::Test' message from identity 'test'.", 0s);

	conn->Disconnect();
	BOOST_CHECK(Shutdown(client));
	CHECK_WITHIN(!server->lowest_layer().is_open(), 1s);
}

BOOST_AUTO_TEST_CASE(messageresult)
{
	auto conn = ConnectEndpoint(server, "test");

	// clang-format off
	Dictionary::Ptr message = new Dictionary({
		{ "jsonrpc", "2.0" },
		{ "method", "test::Test" },
		{ "id", "test1" },
		{ "params", new Dictionary{{"test", "test"}} }
	});
	// clang-format on

	TestApiHandler::RegisterTestFn("test", [&](const TestApiHandler::Message&) { return "Success"; });

	BOOST_REQUIRE_NO_THROW(JsonRpc::SendMessage(client, message));
	client->flush();

	auto msg = JsonRpc::DecodeMessage(JsonRpc::ReadMessage(client));
	BOOST_CHECK_EQUAL(msg->Get("id"), "test1");
	BOOST_CHECK_EQUAL(msg->Get("result"), "Success");

	conn->Disconnect();
	BOOST_CHECK(Shutdown(client));
	CHECK_WITHIN(!server->lowest_layer().is_open(), 1s);
}

BOOST_AUTO_TEST_CASE(messageresult_noparams)
{
	auto conn = ConnectEndpoint(server, "test");

	// clang-format off
	Dictionary::Ptr message = new Dictionary({
		{ "jsonrpc", "2.0" },
		{ "method", "test::Test" },
		{ "id", "test1" }
	});
	// clang-format on

	std::atomic_bool handlerCalled = false;
	TestApiHandler::RegisterTestFn("test", [&](const TestApiHandler::Message&) {
		handlerCalled = true;
		return Empty;
	});

	BOOST_REQUIRE_NO_THROW(JsonRpc::SendMessage(client, message));
	client->flush();

	// Ensure the message has been processed.
	REQUIRE_LOG_MESSAGE("Processed JSON-RPC 'test::Test' message for identity 'test'.*", 1s);

	// The handler must not have been called since our message doesn't have any parameters.
	BOOST_REQUIRE(!handlerCalled);

	auto msg = JsonRpc::DecodeMessage(JsonRpc::ReadMessage(client));
	BOOST_REQUIRE_EQUAL(msg->Get("id"), "test1");
	BOOST_REQUIRE_EQUAL(msg->Get("result"), Empty);

	conn->Disconnect();
	BOOST_CHECK(Shutdown(client));
	CHECK_WITHIN(!server->lowest_layer().is_open(), 1s);
}

BOOST_AUTO_TEST_CASE(call_to_nonexistant_function)
{
	auto conn = ConnectEndpoint(server, "test");

	// clang-format off
	Dictionary::Ptr message = new Dictionary({
		{ "jsonrpc", "2.0" },
		{ "method", "test::FooBar" },
		{ "params", new Dictionary{} }
	});
	// clang-format on

	BOOST_REQUIRE_NO_THROW(JsonRpc::SendMessage(client, message));
	client->flush();

	CHECK_LOG_MESSAGE("Processed JSON-RPC 'test::FooBar' message for identity 'test'.*", 1s);
	CHECK_LOG_MESSAGE("Call to non-existent function.*", 0s);

	conn->Disconnect();
	BOOST_CHECK(Shutdown(client));
	CHECK_WITHIN(!server->lowest_layer().is_open(), 1s);
}

BOOST_AUTO_TEST_CASE(heartbeat_message)
{
	auto conn = ConnectEndpoint(server, "test");
	conn->SetHeartbeatInterval(50ms);

	String msgString;
	CHECK_DONE_BETWEEN(msgString = JsonRpc::ReadMessage(client), 45ms, 55ms);

	Dictionary::Ptr msg;
	BOOST_REQUIRE_NO_THROW(msg = JsonRpc::DecodeMessage(msgString));
	BOOST_CHECK_EQUAL(msg->Get("method"), "event::Heartbeat");

	conn->Disconnect();
	BOOST_CHECK(Shutdown(client));
	CHECK_WITHIN(!server->lowest_layer().is_open(), 1s);
}

BOOST_AUTO_TEST_CASE(anonymous_disconnect)
{
	auto [a, b] = ConnectEndpointPair("a", "b", false);
	BOOST_CHECK(!a->IsAuthenticated());
	BOOST_CHECK(!b->IsAuthenticated());

	// Actual timeout we test here is this value divided by 6, so 50ms.
	a->SetLivenessTimeout(300ms);
	b->SetLivenessTimeout(300ms);

	CHECK_EDGE_WITHIN(!client->lowest_layer().is_open() && !server->lowest_layer().is_open(), 45ms, 55ms);
}

BOOST_AUTO_TEST_CASE(liveness_disconnect)
{
	auto [a, b] = ConnectEndpointPair("a", "b");
	BOOST_CHECK(a->IsAuthenticated());
	BOOST_CHECK(b->IsAuthenticated());

	a->SetLivenessTimeout(50ms);
	b->SetLivenessTimeout(50ms);

	CHECK_EDGE_WITHIN(!client->lowest_layer().is_open() && !server->lowest_layer().is_open(), 45ms, 55ms);
}

/* TODO: This test currently completes successfully, but only because of a scheduling
 *       detail of when the coroutine is spawned vs. when WriteOutgoingMessages() resumes.
 *       Ideally at some point we would want to rethink consistency during shutdown, wait
 *       for ACKs to some important messages and use the waitgroup to wait until the reply
 *       comes in. So currently this is disabled, because we can't reliably assume that
 *       remaining messages will get sent, but also because it doesn't test functionality
 *       we're using in any meaningful way at the moment.
 */
BOOST_AUTO_TEST_CASE(wg_join_during_send, *boost::unit_test::disabled{})
{
	auto conn = ConnectEndpoint(server, "test");
	BOOST_CHECK(conn->IsAuthenticated());

	std::promise<void> beforeWgJoinPromise;
	TestApiHandler::RegisterTestFn("test", [&](const TestApiHandler::Message&) {
		beforeWgJoinPromise.set_value();
		return "Response";
	});

	// clang-format off
	Dictionary::Ptr message = new Dictionary({
		{ "jsonrpc", "2.0" },
		{ "method", "test::Test" },
		{ "id", "wgTest1" },
		{ "params", new Dictionary{{"test", "test"}} }
	});
	// clang-format on

	BOOST_REQUIRE_NO_THROW(JsonRpc::SendMessage(client, message));
	client->flush();

	// Wait until the API-function is running, then join the WaitGroup.
	beforeWgJoinPromise.get_future().get();
	JoinWaitgroup(conn);

	// We still need to receive a response, even though the waitgroup is joined.
	String messageRaw;
	BOOST_REQUIRE_NO_THROW(messageRaw = JsonRpc::ReadMessage(client));
	BOOST_REQUIRE_NO_THROW(message = JsonRpc::DecodeMessage(messageRaw));
	BOOST_CHECK_EQUAL(message->Get("id"), "wgTest1");
	BOOST_CHECK_EQUAL(message->Get("result"), "Response");

	conn->Disconnect();
	BOOST_CHECK(Shutdown(client));
	CHECK_WITHIN(!server->lowest_layer().is_open(), 1s);
}

BOOST_AUTO_TEST_CASE(send_after_wg_join)
{
	auto conn = ConnectEndpoint(server, "test");

	BOOST_CHECK(conn->IsAuthenticated());

	std::atomic_bool handlerRan = false;
	TestApiHandler::RegisterTestFn("test", [&](const TestApiHandler::Message&) {
		handlerRan = true;
		return Empty;
	});

	// clang-format off
	Dictionary::Ptr message = new Dictionary({
		{ "jsonrpc", "2.0" },
		{ "method", "test::Test" },
		{ "id", "wgTest1" },
		{ "params", new Dictionary{{"test", "test"}} }
	});
	// clang-format on

	// Join the wait-group even before sending the message.
	JoinWaitgroup(conn);

	// The message should be received without error, but it should not be processed.
	BOOST_REQUIRE_NO_THROW(JsonRpc::SendMessage(client, message));
	client->flush();

	// Wait until the message has been processed.
	CHECK_LOG_MESSAGE("Processed JSON-RPC 'test::Test' message for identity 'test'.*", 1s);

	// Verify that the handler hasn't run.
	BOOST_CHECK(!handlerRan);

	// Since the message has not been processed, JsonRpcConnection also shouldn't have sent anything.
	BOOST_CHECK_EQUAL(Endpoint::GetByName("test")->GetBytesSentPerSecond(), 0);

	conn->Disconnect();
	BOOST_CHECK(Shutdown(client));
	CHECK_WITHIN(!server->lowest_layer().is_open(), 1s);
}

BOOST_AUTO_TEST_SUITE_END()
