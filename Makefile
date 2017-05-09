PROJECT = rabbitmq_msg_store_kv
PROJECT_DESCRIPTION = ElevelDB based RabbitMQ message store
PROJECT_VERSION = 0.1.0

DEPS = rabbit_common rabbit eleveldb leveled
TEST_DEPS = ct_helper rabbitmq_ct_helpers

dep_leveled = git https://github.com/martinsumner/leveled.git master
dep_eleveldb_commit = 2.0.34

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk
