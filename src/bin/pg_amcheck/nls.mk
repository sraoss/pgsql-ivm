# src/bin/pg_amcheck/nls.mk
CATALOG_NAME     = pg_amcheck
AVAIL_LANGUAGES  = de el es fr zh_CN
GETTEXT_FILES    = $(FRONTEND_COMMON_GETTEXT_FILES) \
                   pg_amcheck.c \
                   ../../fe_utils/cancel.c \
                   ../../fe_utils/connect_utils.c \
                   ../../fe_utils/query_utils.c
GETTEXT_TRIGGERS = $(FRONTEND_COMMON_GETTEXT_TRIGGERS)
GETTEXT_FLAGS    = $(FRONTEND_COMMON_GETTEXT_FLAGS)
