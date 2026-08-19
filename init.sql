-- First, create the role if it doesn't exist
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles WHERE rolname = 'anon'
   ) THEN
      CREATE ROLE anon NOLOGIN;
   END IF;
END
$do$;

-- Ensure the public schema exists and set up permissions
CREATE SCHEMA IF NOT EXISTS public;

-- Search helpers used by API queries and future fuzzy matching
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;

-- Revoke all permissions from public schema
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM PUBLIC;

-- Create the table in the public schema
CREATE TABLE IF NOT EXISTS public.desaparecidos (
    id SERIAL PRIMARY KEY,
    fecha_extraccion DATE NOT NULL,
    url_origen TEXT NOT NULL,
    fecha_modificacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    localizado BOOLEAN DEFAULT FALSE,
    hashid TEXT NOT NULL,
    datos JSONB,
    CONSTRAINT desaparecidos_hashid_localizado_key UNIQUE (hashid, localizado)
);

CREATE INDEX IF NOT EXISTS idx_desaparecidos_hashid
    ON public.desaparecidos (hashid);

CREATE INDEX IF NOT EXISTS idx_desaparecidos_fecha_extraccion
    ON public.desaparecidos (fecha_extraccion);

CREATE INDEX IF NOT EXISTS idx_desaparecidos_datos_gin
    ON public.desaparecidos USING GIN (datos);

CREATE INDEX IF NOT EXISTS idx_desaparecidos_nombre_trgm
    ON public.desaparecidos USING GIN ((lower(datos->>'nombre')) gin_trgm_ops);

-- Grant necessary permissions to anon role
GRANT USAGE ON SCHEMA public TO anon;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO anon;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO anon;

-- Ensure future tables will grant SELECT to anon
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO anon;

-- Grant specific permissions for the desaparecidos table
GRANT SELECT ON public.desaparecidos TO anon;
