/* 1. Query que retorna todos os índices do schema, com nome da tabela e coluna */
SELECT indexname AS indice, tablename AS tabela, regexp_matches(indexdef, '\("([^"]+)"\)', 'g') AS coluna FROM pg_indexes WHERE schemaname = 'public';

/* 2. Procedure que apaga os índices de uma dada tabela */
CREATE OR REPLACE PROCEDURE drop_indexes_from(table_name varchar)
LANGUAGE PLPGSQL
AS $$
DECLARE
    idx varchar;
	indexes CURSOR FOR SELECT indexname, tablename FROM pg_indexes WHERE tablename = table_name;
BEGIN
	FOR idx IN indexes LOOP 
		EXECUTE format('ALTER TABLE "%s" DROP CONSTRAINT IF EXISTS "%s" CASCADE', idx.tablename, idx.indexname);
		EXECUTE format('DROP INDEX IF EXISTS %s."%s"', current_schema, idx.indexname);	
	END LOOP;
END $$;

SELECT indexname, tablename FROM pg_indexes;
CALL drop_indexes_from('Track');

/* 3. Query que retorna todas as foreign keys do schema, com nome da tabela e coluna */
SELECT ccu.constraint_name AS foreign_key, tc.table_name AS tabela, ccu.column_name AS coluna FROM information_schema.constraint_column_usage ccu RIGHT JOIN information_schema.table_constraints tc ON tc.constraint_name = ccu.constraint_name WHERE (ccu.table_schema = current_schema AND constraint_type = 'FOREIGN KEY');

/* 4. TO DO */

--select * from information_schema."tables" t 

select * from information_schema."columns" c where table_schema = 'public' order by c.table_name, c.ordinal_position

select row_to_json(row) from (select array_agg(c.column_name) as table_columns, array_agg(c.data_type) as data_type, array_agg(c.is_nullable) as is_nullabel, array_agg(c.character_maximum_length) as character_maximum_lenght , table_name from information_schema."columns" c where table_schema = 'public' group by table_name) row 


select distinct(c.table_name) from information_schema."columns" c where table_schema = 'public'  --Pegar os nomes das tabelas

--Uma funcao para construir as tabelas e chaves primarias

CREATE OR REPLACE procedure  create_tables_statements_from_schema()
LANGUAGE PLPGSQL
AS $$
DECLARE
	tables_info CURSOR FOR SELECT row_to_json(row) FROM (SELECT array_agg(c.column_name) AS table_columns, array_agg(c.data_type) AS data_type, array_agg(c.is_nullable) AS is_nullabel, array_agg(c.character_maximum_length) AS character_maximum_lenght , table_name FROM information_schema."columns" c WHERE table_schema = 'public' GROUP BY table_name) ROW;
BEGIN
	
	
	for table_info in tables_info LOOP
		RAISE NOTICE 'Tabela: %', table_info;
		RAISE NOTICE 'CREATE TABLE "%" ( )'
	END LOOP;
	
END $$;


CALL create_tables_statements_from_schema()
--Uma funcao para criar as chaves estrangeiras nas tabelas

/* 5. Maquina de Estados da coluna status da tabela Track */
ALTER TABLE "Track" ADD status VARCHAR;

CREATE OR REPLACE TRIGGER 
track_state_transition
BEFORE INSERT OR UPDATE OF status ON "Track"
FOR EACH ROW
EXECUTE PROCEDURE check_state_transition();


CREATE OR REPLACE FUNCTION check_state_transition() RETURNS trigger as $check_state_transition$
BEGIN
	IF OLD.status = NEW.status THEN
		RAISE EXCEPTION 'No transition needed' USING HINT = format('State already is %s.', OLD.status);
	END IF;
	IF OLD.status = 'created' THEN
		IF NEW.status = 'approved' THEN
			RAISE EXCEPTION 'Unauthorized transition' USING HINT = 'Cant transition from created to approved.';
		ELSIF NEW.status <> 'in_analysis' THEN
			RAISE EXCEPTION 'Unauthorized transition' USING HINT = 'This state does not exist.';
		END IF;
	ELSIF OLD.status = 'approved' THEN
		IF NEW.status = 'created' THEN
			RAISE EXCEPTION 'Unauthorized transition' USING HINT = 'Cant transition from approved to created.';
		ELSIF NEW.status <> 'in_analysis' THEN
			RAISE EXCEPTION 'Unauthorized transition' USING HINT = 'This state does not exist.';
		END IF;
	ELSIF OLD.status = 'in_analysis' THEN
		IF NEW.status = 'created' THEN
			RAISE EXCEPTION 'Unauthorized transition' USING HINT = 'Cant transition from in_analysis to created.';
		ELSIF NEW.status <> 'approved' THEN
			RAISE EXCEPTION 'Unauthorized transition' USING HINT = 'This state does not exist.';
		END IF;
	END IF;
  	RETURN NEW;
END;
$check_state_transition$ LANGUAGE plpgsql;



