/* PARTE 1 */
/* 1. Query que retorna todos os índices do schema, com nome da tabela e coluna */
SELECT
    indexname AS indice,
    tablename AS tabela,
    regexp_matches(indexdef, '\("([^"]+)"\)', 'g') AS coluna
FROM
    pg_indexes
WHERE
    schemaname = 'public';


/* 2. Procedure que apaga os índices de uma dada tabela */
CREATE OR REPLACE PROCEDURE drop_indexes_from (table_name varchar)
LANGUAGE PLPGSQL
AS $$
DECLARE
    idx varchar;
    indexes CURSOR FOR
        SELECT
            indexname,
            tablename
        FROM
            pg_indexes
        WHERE
            tablename = table_name;
BEGIN
    FOR idx IN indexes LOOP
        EXECUTE format('ALTER TABLE "%s" DROP CONSTRAINT IF EXISTS "%s" CASCADE', idx.tablename, idx.indexname);
        EXECUTE format('DROP INDEX IF EXISTS %s."%s"', current_schema, idx.indexname);
    END LOOP;
END
$$;

SELECT
    indexname,
    tablename
FROM
    pg_indexes;

CALL drop_indexes_from ('Track');


/* 3. Query que retorna todas as foreign keys do schema, com nome da tabela e coluna */
SELECT
    ccu.constraint_name AS foreign_key,
    tc.table_name AS tabela,
    ccu.column_name AS coluna
FROM
    information_schema.constraint_column_usage ccu
    RIGHT JOIN information_schema.table_constraints tc ON tc.constraint_name = ccu.constraint_name
WHERE (ccu.table_schema = current_schema
    AND constraint_type = 'FOREIGN KEY');


/* 4. Procedure que passado o nome do schema que está o seu Chinook, faz uma cópia e imprime no output o código de criação das tabelas */
CREATE OR REPLACE PROCEDURE clone_schema (schema_name varchar)
LANGUAGE PLPGSQL
AS $$
DECLARE
    tables_info CURSOR FOR
        SELECT
            array_agg(c.column_name) AS table_columns,
            array_agg(c.numeric_precision) AS numeric_precision,
            array_agg(c.numeric_scale) AS numeric_scale,
            array_agg(c.data_type) AS data_type,
            array_agg(c.is_nullable) AS is_nullable,
            array_agg(c.character_maximum_length) AS character_maximum_lenght,
            table_name
        FROM
            information_schema."columns" c
        WHERE
            table_schema = schema_name
        GROUP BY
            table_name;
    pks_info CURSOR FOR
        SELECT
            ccu.constraint_name AS primary_key,
            tc.table_name AS tabela,
            ccu.column_name AS coluna
        FROM
            information_schema.constraint_column_usage ccu
        RIGHT JOIN information_schema.table_constraints tc ON tc.constraint_name = ccu.constraint_name
    WHERE (ccu.table_schema = schema_name
        AND constraint_type = 'PRIMARY KEY');
    fks_info CURSOR FOR
        SELECT
            ccu.constraint_name AS foreign_key,
            tc.table_name AS tabela_origem,
            ccu.table_name AS tabela_destino,
            kcu.column_name AS coluna_origem,
            ccu.column_name AS coluna_destino
        FROM
            information_schema.constraint_column_usage ccu
        RIGHT JOIN information_schema.table_constraints tc ON tc.constraint_name = ccu.constraint_name
        LEFT JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
    WHERE (kcu.table_schema = schema_name
        AND constraint_type = 'FOREIGN KEY');
    i integer;
    create_statement varchar;
    column_name varchar;
    column_type varchar;
    is_column_nullable varchar;
    charcter_maximum_lenght integer;
    number_of_columns integer;
    column_definition varchar;
    numeric_scale integer;
    numeric_precision integer;
    pk_definition varchar;
    fk_definition varchar;
    colunas varchar;
    count_pk integer;
    col varchar;
    new_schema_name varchar;
    BEGIN
        new_schema_name := 'copy_of_' || schema_name;
        EXECUTE format('DROP SCHEMA IF EXISTS %s CASCADE', new_schema_name);
        EXECUTE format('CREATE SCHEMA %s', new_schema_name);
        FOR table_info IN tables_info LOOP
            create_statement := format('CREATE TABLE %s."%s" (', new_schema_name, table_info.table_name);
            SELECT
                count(*) INTO number_of_columns
            FROM
                information_schema."columns"
            WHERE
                table_schema = schema_name
                AND table_name = table_info.table_name;
            FOR i IN 1..number_of_columns LOOP
                column_definition := '';
                column_name := table_info.table_columns[i];
                column_type := table_info.data_type[i];
                is_column_nullable := table_info.is_nullable[i];
                charcter_maximum_lenght := table_info.character_maximum_lenght[i];
                numeric_precision := table_info.numeric_precision[i];
                numeric_scale := table_info.numeric_scale[i];
                column_definition := concat(column_definition, format('"%s"', column_name));
                IF column_type = 'integer' THEN
                    column_definition := concat(column_definition, ' ', 'INT');
                END IF;
                IF column_type = 'character varying' THEN
                    column_definition := concat(column_definition, ' ', format('VARCHAR(%s)', charcter_maximum_lenght::varchar));
                END IF;
                IF column_type = 'timestamp without time zone' THEN
                    column_definition := concat(column_definition, ' ', 'TIMESTAMP');
                END IF;
                IF column_type = 'numeric' THEN
                    column_definition := concat(column_definition, ' ', format('NUMERIC(%s,%s)', numeric_precision::varchar, numeric_scale::varchar));
                END IF;
                IF is_column_nullable = 'NO' THEN
                    column_definition := concat(column_definition, ' ', 'NOT NULL');
                END IF;
                IF i <> number_of_columns THEN
                    column_definition := concat(column_definition, ',');
                END IF;
                create_statement := concat(create_statement, column_definition);
            END LOOP;
            create_statement := concat(create_statement, ');');
            RAISE NOTICE '%', create_statement;
            EXECUTE create_statement;
        END LOOP;
        FOR pk_info IN pks_info LOOP
            SELECT
                count(*) INTO count_pk
            FROM (
                SELECT
                    ccu.constraint_name AS primary_key,
                    tc.table_name AS tabela,
                    ccu.column_name AS coluna
                FROM
                    information_schema.constraint_column_usage ccu
                RIGHT JOIN information_schema.table_constraints tc ON tc.constraint_name = ccu.constraint_name
            WHERE (ccu.table_schema = schema_name
                AND constraint_type = 'PRIMARY KEY')) a
        WHERE
            tabela = pk_info.tabela;
            IF count_pk > 1 THEN
                EXECUTE format('ALTER TABLE %s."%s" DROP CONSTRAINT IF EXISTS "%s"', new_schema_name, pk_info.tabela, pk_info.primary_key);
                colunas = '';
                FOR col IN
                SELECT
                    coluna
                FROM (
                    SELECT
                        ccu.constraint_name AS primary_key,
                        tc.table_name AS tabela,
                        ccu.column_name AS coluna
                    FROM
                        information_schema.constraint_column_usage ccu
                    RIGHT JOIN information_schema.table_constraints tc ON tc.constraint_name = ccu.constraint_name
                WHERE (ccu.table_schema = schema_name
                    AND constraint_type = 'PRIMARY KEY')) b
            WHERE
                tabela = pk_info.tabela LOOP
                    IF colunas <> '' THEN
                        colunas := concat(colunas, format(', "%s"', col));
                    ELSE
                        colunas := concat(colunas, format('"%s"', col));
                    END IF;
                END LOOP;
                pk_definition := format('ALTER TABLE %s."%s" ADD CONSTRAINT "%s" PRIMARY KEY (%s);', new_schema_name, pk_info.tabela, pk_info.primary_key, colunas);
                RAISE NOTICE '%', pk_definition;
                EXECUTE pk_definition;
            ELSE
                pk_definition := format('ALTER TABLE %s."%s" ADD CONSTRAINT "%s" PRIMARY KEY ("%s");', new_schema_name, pk_info.tabela, pk_info.primary_key, pk_info.coluna);
                RAISE NOTICE '%', pk_definition;
                EXECUTE pk_definition;
            END IF;
        END LOOP;
        FOR fk_info IN fks_info LOOP
            fk_definition := format('ALTER TABLE %s."%s" ADD CONSTRAINT "%s" FOREIGN KEY ("%s") REFERENCES %s."%s" ("%s");', new_schema_name, fk_info.tabela_origem, fk_info.foreign_key, fk_info.coluna_origem, new_schema_name, fk_info.tabela_destino, fk_info.coluna_destino);
            RAISE NOTICE '%', fk_definition;
            EXECUTE fk_definition;
        END LOOP;
    END
$$;

CALL clone_schema ('public');

-- substituir nome do seu schema
/* 5. Maquina de Estados da coluna status da tabela Track */
ALTER TABLE "Track"
    ADD status VARCHAR(20);

CREATE OR REPLACE TRIGGER track_state_transition BEFORE INSERT
    OR UPDATE OF status ON "Track" FOR EACH ROW EXECUTE PROCEDURE check_state_transition ();

CREATE OR REPLACE FUNCTION check_state_transition ()
    RETURNS TRIGGER
    AS $check_state_transition$
BEGIN
    IF OLD.status = NEW.status THEN
        RAISE EXCEPTION 'No transition needed'
            USING HINT = format('State already is %s.', OLD.status);
        END IF;
        IF OLD.status = 'created' THEN
            IF NEW.status = 'approved' THEN
                RAISE EXCEPTION 'Unauthorized transition'
                    USING HINT = 'Cant transition from created to approved.';
                ELSIF NEW.status <> 'in_analysis' THEN
                    RAISE EXCEPTION 'Unauthorized transition'
                        USING HINT = 'This state does not exist.';
                    END IF;
                ELSIF OLD.status = 'approved' THEN
                    IF NEW.status = 'created' THEN
                        RAISE EXCEPTION 'Unauthorized transition'
                            USING HINT = 'Cant transition from approved to created.';
                        ELSIF NEW.status <> 'in_analysis' THEN
                            RAISE EXCEPTION 'Unauthorized transition'
                                USING HINT = 'This state does not exist.';
                            END IF;
                        ELSIF OLD.status = 'in_analysis' THEN
                            IF NEW.status = 'created' THEN
                                RAISE EXCEPTION 'Unauthorized transition'
                                    USING HINT = 'Cant transition from in_analysis to created.';
                                ELSIF NEW.status <> 'approved' THEN
                                    RAISE EXCEPTION 'Unauthorized transition'
                                        USING HINT = 'This state does not exist.';
                                    END IF;
                                END IF;
                                RETURN NEW;
END;
$check_state_transition$
LANGUAGE plpgsql;


/* PARTE 2 */
/* 1. TO DO */
/* 2. Trigger que garante que uma track que não esteja Approved não pode ser adicionada à uma invoice */
CREATE OR REPLACE TRIGGER track_state_check BEFORE INSERT
    OR UPDATE ON "InvoiceLine" FOR EACH ROW EXECUTE PROCEDURE check_track_state ();

CREATE OR REPLACE FUNCTION check_track_state ()
    RETURNS TRIGGER
    AS $check_track_state$
DECLARE
    track_status "Track".status%TYPE;
BEGIN
    SELECT
        status INTO track_status
    FROM
        "Track" t
    WHERE
        "TrackId" = NEW."TrackId";
    IF track_status <> 'approved' THEN
        RAISE EXCEPTION 'This track cannot be added to an invoice'
            USING HINT = format('Cannot add a track which state is %s', track_status);
        END IF;
        RETURN NEW;
END;
$check_track_state$
LANGUAGE plpgsql;

-- Funciona
UPDATE
    "Track"
SET
    status = 'approved'
WHERE
    "TrackId" = 1;

INSERT INTO "InvoiceLine" ("InvoiceId", "TrackId", "UnitPrice", "Quantity")
    VALUES (1, 1, 0.99, 1);

-- Erro, track não pode ser adicionada pois não está approved
UPDATE
    "Track"
SET
    status = 'created'
WHERE
    "TrackId" = 2;

INSERT INTO "InvoiceLine" ("InvoiceId", "TrackId", "UnitPrice", "Quantity")
    VALUES (1, 2, 0.99, 1);

