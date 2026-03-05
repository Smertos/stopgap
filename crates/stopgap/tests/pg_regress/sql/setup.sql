-- this setup file is run immediately after the regression database is (re)created
-- the file is optional but you likely want to create the extension
DROP EXTENSION IF EXISTS stopgap CASCADE;
DROP EXTENSION IF EXISTS plts CASCADE;
CREATE EXTENSION plts;
CREATE EXTENSION stopgap;
