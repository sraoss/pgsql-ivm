# src/bin/psql/nls.mk
CATALOG_NAME     = psql
AVAIL_LANGUAGES  = cs de es fr he it ja ko pl pt_BR ru sv tr uk zh_CN zh_TW
GETTEXT_FILES    = $(FRONTEND_COMMON_GETTEXT_FILES) \
                   command.c common.c copy.c crosstabview.c help.c input.c large_obj.c \
                   mainloop.c psqlscanslash.c startup.c \
                   describe.c sql_help.h sql_help.c \
                   tab-complete.c variables.c \
                   ../../fe_utils/cancel.c ../../fe_utils/print.c ../../fe_utils/psqlscan.c \
                   ../../common/exec.c ../../common/fe_memutils.c ../../common/username.c \
                   ../../common/wait_error.c
GETTEXT_TRIGGERS = $(FRONTEND_COMMON_GETTEXT_TRIGGERS) \
                   N_ simple_prompt
GETTEXT_FLAGS    = $(FRONTEND_COMMON_GETTEXT_FLAGS)
