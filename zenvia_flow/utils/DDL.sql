CREATE TABLE zenvia.relatorio_chamadas (
    id nvarchar(55) null,
    id_ramal_origem nvarchar(55) null,
    dt_criacao date null,
    destino_status nvarchar(55) null,
    destino_numero nvarchar(255) null,
    destino_duracao_segundos int null,
    destino_duracao_falada_segundos int null,
    ramal_origem_login nvarchar(max) null,
    url_gravacao nvarchar(max) null,
    inserted_at datetime,
)
-- ['id','dt_criacao','id_ramal_origem','destino_numero','destino_status','destino_duracao_segundos','destino_duracao_falada_segundos', 'ramal_origem_login', 'url_gravacao']
-- data != datetime
-- date 2023-08-18
-- https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver16