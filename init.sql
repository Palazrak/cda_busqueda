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
    datos JSONB
);

-- Grant necessary permissions to anon role
GRANT USAGE ON SCHEMA public TO anon;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO anon;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO anon;

-- Ensure future tables will grant SELECT to anon
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO anon;

-- Grant specific permissions for the desaparecidos table
GRANT SELECT ON public.desaparecidos TO anon;