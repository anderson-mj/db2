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

/* 3. TO DO */

SELECT * FROM information_schema.table_constraints tc  WHERE table_schema = current_schema AND constraint_type = 'FOREIGN KEY';

