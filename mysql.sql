/* 1. Query que retorna todos os índices do schema, com nome da tabela e coluna */
SELECT INDEX_NAME AS 'index', TABLE_NAME AS 'tabela', COLUMN_NAME AS 'coluna' FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = 'Chinook';

/* 2. Procedure que apaga os índices de uma dada tabela */

DROP PROCEDURE IF EXISTS drop_indexes_from;
CREATE PROCEDURE drop_indexes_from (pval INT)
BEGIN
  DECLARE idx; -- TO DO
END;
