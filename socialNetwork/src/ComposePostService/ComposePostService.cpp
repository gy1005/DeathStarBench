#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <signal.h>

#include "../utils.h"
#include "ComposePostHandler.h"


using apache::thrift::server::TThreadedServer;
using apache::thrift::transport::TServerSocket;
using apache::thrift::transport::TFramedTransportFactory;
using apache::thrift::protocol::TBinaryProtocolFactory;
using namespace social_network;

void sigintHandler(int sig) {
  exit(EXIT_SUCCESS);
}

int main(int argc, char *argv[]) {
  signal(SIGINT, sigintHandler);
  init_logger();
  SetUpTracer("config/jaeger-config.yml", "compose-post-service");

  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  int port = config_json["compose-post-service"]["port"];

  int rabbitmq_port = config_json["write-home-timeline-rabbitmq"]["port"];
  std::string rabbitmq_addr = config_json["write-home-timeline-rabbitmq"]["addr"];
  int rabbitmq_conns = config_json["write-home-timeline-rabbitmq"]["connections"];
  int rabbitmq_timeout = config_json["write-home-timeline-rabbitmq"]["timeout_ms"];

  int post_storage_port = config_json["post-storage-service"]["port"];
  std::string post_storage_addr = config_json["post-storage-service"]["addr"];
  int post_storage_conns = config_json["post-storage-service"]["connections"];
  int post_storage_timeout = config_json["post-storage-service"]["timeout_ms"];

  int user_timeline_port = config_json["user-timeline-service"]["port"];
  std::string user_timeline_addr = config_json["user-timeline-service"]["addr"];
  int user_timeline_conns = config_json["user-timeline-service"]["connections"];
  int user_timeline_timeout = config_json["user-timeline-service"]["timeout_ms"];

  int text_port = config_json["text-service"]["port"];
  std::string text_addr = config_json["text-service"]["addr"];
  int text_conns = config_json["text-service"]["connections"];
  int text_timeout = config_json["text-service"]["timeout_ms"];

  int user_port = config_json["user-service"]["port"];
  std::string user_addr = config_json["user-service"]["addr"];
  int user_conns = config_json["user-service"]["connections"];
  int user_timeout = config_json["user-service"]["timeout_ms"];

  int media_port = config_json["media-service"]["port"];
  std::string media_addr = config_json["media-service"]["addr"];
  int media_conns = config_json["media-service"]["connections"];
  int media_timeout = config_json["media-service"]["timeout_ms"];

  int unique_id_port = config_json["unique-id-service"]["port"];
  std::string unique_id_addr = config_json["unique-id-service"]["addr"];
  int unique_id_conns = config_json["unique-id-service"]["connections"];
  int unique_id_timeout = config_json["unique-id-service"]["timeout_ms"];


  ClientPool<ThriftClient<PostStorageServiceClient>>
      post_storage_client_pool("post-storage-client", post_storage_addr,
                               post_storage_port, 0, post_storage_conns, post_storage_timeout);
  ClientPool<ThriftClient<UserTimelineServiceClient>>
      user_timeline_client_pool("user-timeline-client", user_timeline_addr,
                                user_timeline_port, 0, user_timeline_conns, user_timeline_timeout);
  ClientPool<ThriftClient<TextServiceClient>>
      text_client_pool("text-service-client",text_addr,
                       text_port, 0, text_conns, text_timeout);
  ClientPool<ThriftClient<UserServiceClient>>
      user_client_pool("user-service-client", user_addr,
                       user_port, 0, user_conns, user_timeout);
  ClientPool<ThriftClient<MediaServiceClient>>
      media_client_pool("media-service-client", media_addr,
                        media_port, 0, media_conns, media_timeout);
  ClientPool<ThriftClient<UniqueIdServiceClient>>
      unique_id_client_pool("unique-id-service-client", unique_id_addr,
                                unique_id_port, 0, unique_id_conns, unique_id_timeout);

  ClientPool<RabbitmqClient> rabbitmq_client_pool("rabbitmq", rabbitmq_addr,
      rabbitmq_port, 0, rabbitmq_conns, rabbitmq_timeout);

  TThreadedServer server(
      std::make_shared<ComposePostServiceProcessor>(
          std::make_shared<ComposePostHandler>(
              &post_storage_client_pool,
              &user_timeline_client_pool,
              &user_client_pool,
              &unique_id_client_pool,
              &media_client_pool,
              &text_client_pool,
              &rabbitmq_client_pool)),
      std::make_shared<TServerSocket>("0.0.0.0", port),
      std::make_shared<TFramedTransportFactory>(),
      std::make_shared<TBinaryProtocolFactory>()
  );
  std::cout << "Starting the compose-post-service server ..." << std::endl;
  server.serve();

}