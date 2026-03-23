"""partition events and rawlogs by day

Revision ID: 7015555057f7
Revises: 2fbacf9e5a2f
Create Date: 2026-02-23

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "7015555057f7"
down_revision = "2fbacf9e5a2f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 0) Cortar FK que se vuelve inválido cuando events deja de tener PK(id) global
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'rule_window_events_event_id_fkey'
          ) THEN
            ALTER TABLE public.rule_window_events
              DROP CONSTRAINT rule_window_events_event_id_fkey;
          END IF;
        END $$;
        """
    )

    # 1) Quitar FK events.raw_id -> rawlogs.id si existe (al particionar rawlogs ya no es viable)
    op.execute(
        """
        DO $$
        DECLARE cname text;
        BEGIN
          SELECT conname INTO cname
          FROM pg_constraint
          WHERE conrelid = 'public.events'::regclass
            AND contype = 'f'
            AND pg_get_constraintdef(oid) LIKE '%FOREIGN KEY (raw_id)%rawlogs%';

          IF cname IS NOT NULL THEN
            EXECUTE format('ALTER TABLE public.events DROP CONSTRAINT %I', cname);
          END IF;
        END $$;
        """
    )

    # 2) Renombrar tablas actuales a *_old
    op.execute("ALTER TABLE public.events RENAME TO events_old;")
    op.execute("ALTER TABLE public.rawlogs RENAME TO rawlogs_old;")

    # 3) Crear nuevas tablas particionadas (mismo nombre lógico)
    #    Nota: en DB el PK debe incluir la columna de partición (timestamp_utc / created_at)
    op.execute(
        """
        CREATE TABLE public.events (
          id                  uuid NOT NULL,
          timestamp_utc       timestamptz NOT NULL,
          server              varchar(255) NOT NULL,
          source              varchar(255) NOT NULL,
          service             varchar(255) NOT NULL,
          ip_client           inet,
          ip_server           inet,
          domain              text,
          username            text,
          message             text NOT NULL,
          extra               jsonb NOT NULL DEFAULT '{}'::jsonb,
          created_at          timestamptz NOT NULL DEFAULT now(),
          log_upload_id       integer,
          raw_id              bigint,
          engine_status       varchar(32) NOT NULL DEFAULT 'pending',
          engine_claimed_at   timestamptz,
          engine_processed_at timestamptz,
          engine_attempts     integer NOT NULL DEFAULT 0,
          engine_error        text,
          PRIMARY KEY (timestamp_utc, id),
          CONSTRAINT events_log_upload_id_fkey
            FOREIGN KEY (log_upload_id) REFERENCES public.log_uploads(id) ON DELETE SET NULL
        ) PARTITION BY RANGE (timestamp_utc);
        """
    )

    op.execute(
        """
        CREATE TABLE public.rawlogs (
          id            bigint NOT NULL DEFAULT nextval('rawlogs_id_seq'::regclass),
          server        varchar(255) NOT NULL,
          source_hint   varchar(64)  NOT NULL,
          raw           text NOT NULL,
          created_at    timestamptz NOT NULL DEFAULT now(),
          log_upload_id integer,
          line_no       integer,
          extra         jsonb NOT NULL DEFAULT '{}'::jsonb,
          PRIMARY KEY (created_at, id),
          CONSTRAINT rawlogs_log_upload_id_fkey
            FOREIGN KEY (log_upload_id) REFERENCES public.log_uploads(id) ON DELETE SET NULL
        ) PARTITION BY RANGE (created_at);
        """
    )

    # 4) Crear particiones (UTC) desde hoy-40 hasta hoy+2
    op.execute(
        """
        DO $$
        DECLARE
          d date;
          start_date date := (now() at time zone 'UTC')::date - 40;
          end_date   date := (now() at time zone 'UTC')::date + 2;
          ev_part text;
          rl_part text;
        BEGIN
          d := start_date;
          WHILE d <= end_date LOOP
            ev_part := format('events_%s', to_char(d, 'YYYY_MM_DD'));
            EXECUTE format(
              'CREATE TABLE IF NOT EXISTS public.%I PARTITION OF public.events FOR VALUES FROM (%L) TO (%L);',
              ev_part, d::timestamptz, (d + 1)::timestamptz
            );

            rl_part := format('rawlogs_%s', to_char(d, 'YYYY_MM_DD'));
            EXECUTE format(
              'CREATE TABLE IF NOT EXISTS public.%I PARTITION OF public.rawlogs FOR VALUES FROM (%L) TO (%L);',
              rl_part, d::timestamptz, (d + 1)::timestamptz
            );

            d := d + 1;
          END LOOP;
        END $$;
        """
    )

    # 5) Índices en padres (se propagan a particiones nuevas)
    op.execute("CREATE INDEX IF NOT EXISTS ix_events_timestamp_utc ON public.events (timestamp_utc);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_events_server ON public.events (server);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_events_source ON public.events (source);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_events_service ON public.events (service);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_events_ip_client ON public.events (ip_client);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_events_log_upload_id ON public.events (log_upload_id);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_events_raw_id ON public.events (raw_id);")

    op.execute("CREATE INDEX IF NOT EXISTS ix_events_server_timestamp ON public.events (server, timestamp_utc);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_events_source_timestamp ON public.events (source, timestamp_utc);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_events_service_timestamp ON public.events (service, timestamp_utc);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_events_ip_client_timestamp ON public.events (ip_client, timestamp_utc);")

    op.execute("CREATE INDEX IF NOT EXISTS ix_events_engine_claimed_at ON public.events (engine_claimed_at);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_events_engine_status_created_at ON public.events (engine_status, created_at);")

    op.execute("CREATE INDEX IF NOT EXISTS rawlogs_log_upload_id_idx ON public.rawlogs (log_upload_id);")
    op.execute(
        "CREATE INDEX IF NOT EXISTS rawlogs_log_upload_id_id_idx ON public.rawlogs (log_upload_id, id) WHERE log_upload_id IS NOT NULL;"
    )
    op.execute("CREATE INDEX IF NOT EXISTS rawlogs_server_created_at_idx ON public.rawlogs (server, created_at);")
    op.execute("CREATE INDEX IF NOT EXISTS rawlogs_source_hint_created_at_idx ON public.rawlogs (source_hint, created_at);")

    # 6) Migrar datos (si tu volumen crece, conviene hacerlo por días/batches, pero hoy ~5GB es aceptable)
    op.execute(
        """
        INSERT INTO public.rawlogs (id, server, source_hint, raw, created_at, log_upload_id, line_no, extra)
        SELECT id, server, source_hint, raw, created_at, log_upload_id, line_no, extra
        FROM public.rawlogs_old;
        """
    )

    op.execute(
        """
        INSERT INTO public.events (
          id, timestamp_utc, server, source, service, ip_client, ip_server, domain, username,
          message, extra, created_at, log_upload_id, raw_id,
          engine_status, engine_claimed_at, engine_processed_at, engine_attempts, engine_error
        )
        SELECT
          id, timestamp_utc, server, source, service, ip_client, ip_server, domain, username,
          message, extra, created_at, log_upload_id, raw_id,
          engine_status, engine_claimed_at, engine_processed_at, engine_attempts, engine_error
        FROM public.events_old;
        """
    )

    # 7) Ajustar secuencia rawlogs_id_seq
    op.execute(
        """
        SELECT setval('rawlogs_id_seq', COALESCE((SELECT max(id) FROM public.rawlogs_old), 1), true);
        """
    )

    # 8) Dejar tablas old como respaldo (puedes dropearlas luego manualmente)
    #    Si quieres borrarlas en la migración, descomenta:
    # op.execute("DROP TABLE public.events_old;")
    # op.execute("DROP TABLE public.rawlogs_old;")


def downgrade() -> None:
    raise RuntimeError("Downgrade no soportado para migración de particionado.")
