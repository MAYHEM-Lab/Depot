FROM rabbitmq:3.10.5-management
EXPOSE 5672 15672

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y curl

RUN curl -fsSL \
	-o "$RABBITMQ_HOME/plugins/rabbitmq_delayed_message_exchange-3.10.2.ez" \
	https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases/download/3.10.2/rabbitmq_delayed_message_exchange-3.10.2.ez

RUN chown rabbitmq:rabbitmq $RABBITMQ_HOME/plugins/rabbitmq_delayed_message_exchange-3.10.2.ez

RUN rabbitmq-plugins enable --offline rabbitmq_delayed_message_exchange
RUN rabbitmq-plugins enable --offline rabbitmq_consistent_hash_exchange
