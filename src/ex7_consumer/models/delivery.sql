WITH reclamacoes_dumped AS (
	SELECT
		TRIM(REPLACE(CAST(body->'ano' AS VARCHAR), '"', '')) AS ano,
		TRIM(REPLACE(CAST(body->'trimestre' AS VARCHAR), '"', '')) AS trimestre,
		TRIM(REPLACE(CAST(body->'categoria' AS VARCHAR), '"', '')) AS categoria,
		TRIM(REPLACE(CAST(body->'tipo' AS VARCHAR), '"', ''))  AS tipo,
		TRIM(REPLACE(CAST(body->'cnpj_if' AS VARCHAR), '"', '')) AS cnpj_if,
		TRIM(REPLACE(CAST(body->'instituicao_financeira' AS VARCHAR), '"', '')) AS instituicao_financeira,
		TRIM(REPLACE(CAST(body->'indice' AS VARCHAR), '"', '')) AS indice,
		TRIM(REPLACE(CAST(body->'quantidade_de_reclamacoes_reguladas_procedentes' AS VARCHAR), '"', '')) AS qtd_reclamacoes_reguladas_procedentes,
		TRIM(REPLACE(CAST(body->'quantidade_de_reclamacoes_reguladas_outras' AS VARCHAR), '"', '')) AS qtd_reclamacoes_reguladas_outras,
		TRIM(REPLACE(CAST(body->'quantidade_de_reclamacoes_nao_reguladas' AS VARCHAR), '"', '')) AS qtd_reclamacoes_nao_reguladas,
		TRIM(REPLACE(CAST(body->'quantidade_total_de_reclamacoes' AS VARCHAR), '"', '')) AS qtd_total_reclamacoes,
		TRIM(REPLACE(CAST(body->'quantidade_total_de_clientes_ccs_e_scr' AS VARCHAR), '"', '')) AS qtd_total_clientes_ccs_e_scr,
		TRIM(REPLACE(CAST(body->'quantidade_de_clientes_ccs' AS VARCHAR), '"', '')) AS qtd_clientes_ccs,
		TRIM(REPLACE(CAST(body->'quantidade_de_clientes_scr' AS VARCHAR), '"', '')) AS qtd_clientes_scr,
		md5_body,
		created_at,
		ROW_NUMBER() OVER(PARTITION BY md5_body ORDER BY created_at DESC) AS RN
	FROM ex7.events.reclamacoes_sqs_events
),
reclamacoes AS (
	SELECT 
		CAST(ano AS INTEGER) AS ano, 
		trimestre,
		categoria,
		tipo,
		CASE
			WHEN TRIM(CAST(cnpj_if AS VARCHAR)) = '' THEN NULL
			ELSE LPAD(TRIM(CAST(cnpj_if AS VARCHAR)), 8, '0') 
		END AS cnpj,
		TRIM(REGEXP_REPLACE(UPPER(TRIM(instituicao_financeira)), e'\s*(S\s*\.?\s*A\s*\.?((\s*-\s*)?\s*CFI)?|LTDA\.?|- CFI - PRUDENCIAL|- CFI|- PRUDENCIAL|S\s*\.?\s*A\s*\.?\ - PRUDENCIAL|\\(CONGLOMERADO\\))$', '')) AS instituicao_financeira,
		CAST(REPLACE(REPLACE(TRIM(CASE WHEN indice != '' THEN indice END), '.', ''), ',', '.')  AS NUMERIC) AS indice,
		-- REGEXP_LIKE(REPLACE(TRIM(CASE WHEN indice != '' THEN indice END), ',', '.'), '^\d+\.?\d*') AS is_numeric,
		CAST(CASE WHEN qtd_reclamacoes_reguladas_procedentes != '' THEN qtd_reclamacoes_reguladas_procedentes END AS INTEGER) AS qtd_reclamacoes_reguladas_procedentes, 
		CAST(CASE WHEN qtd_reclamacoes_reguladas_outras != '' THEN qtd_reclamacoes_reguladas_outras END AS INTEGER) AS qtd_reclamacoes_reguladas_outras,
		CAST(CASE WHEN qtd_reclamacoes_nao_reguladas != '' THEN qtd_reclamacoes_nao_reguladas END AS INTEGER) AS qtd_reclamacoes_nao_reguladas,
		CAST(CASE WHEN qtd_total_clientes_ccs_e_scr != '' THEN qtd_total_clientes_ccs_e_scr END AS INTEGER) AS qtd_total_clientes_ccs_e_scr,
		CAST(CASE WHEN qtd_clientes_ccs != '' THEN qtd_clientes_ccs END AS INTEGER) AS qtd_clientes_ccs,
		CAST(CASE WHEN qtd_clientes_scr != '' THEN qtd_clientes_scr END AS INTEGER) AS qtd_clientes_scr,
		CAST(CASE WHEN qtd_total_reclamacoes != '' THEN qtd_total_reclamacoes END AS INTEGER) AS qtd_total_reclamacoes
	FROM reclamacoes_dumped
	WHERE RN = 1
),
rereclamacoes_agg AS (
	SELECT
		ano,
		trimestre,
		categoria,
		tipo,
		cnpj,
		instituicao_financeira,
		AVG(indice) AS indice,
		SUM(qtd_reclamacoes_reguladas_procedentes) AS qtd_reclamacoes_reguladas_procedentes,
		SUM(qtd_reclamacoes_reguladas_outras) AS qtd_reclamacoes_reguladas_outras,
		SUM(qtd_reclamacoes_nao_reguladas) AS qtd_reclamacoes_nao_reguladas,
		SUM(qtd_total_clientes_ccs_e_scr) AS qtd_total_clientes_ccs_e_scr,
		SUM(qtd_clientes_ccs) AS qtd_clientes_ccs,
		SUM(qtd_clientes_scr) AS qtd_clientes_scr,
		SUM(qtd_total_reclamacoes) AS qtd_total_reclamacoes
	FROM reclamacoes
	GROUP BY ano,
		trimestre,
		categoria,
		tipo,
		cnpj,
		instituicao_financeira
),
emp_bco_join AS (
	SELECT 
		employer_name, 
		reviews_count, 
		culture_count, 
		salaries_count, 
		benefits_count, 
		employerwebsite, 
		employerheadquarters, 
		employerfounded, 
		employerindustry, 
		employerrevenue, 
		url, 
		geral, 
		cultura_e_valores, 
		diversidade_e_inclusao, 
		qualidade_de_vida, 
		alta_lideranca, 
		remuneracao_e_beneficios, 
		oportunidades_de_carreira, 
		recomendam_para_outras_pessoas, 
		perspectiva_positiva_da_empresa,
		COALESCE(bco.segmento, emp.segmento) AS segmento,
		COALESCE(bco.cnpj, emp.cnpj) AS cnpj,
		COALESCE(bco.nome, emp.nome) AS nome
	FROM ex7.trusted.empregados AS emp
	LEFT JOIN ex7.trusted.bancos AS bco
		ON NULLIF(emp.cnpj, '') = NULLIF(bco.cnpj, '')
		OR NULLIF(emp.nome, '') = NULLIF(bco.nome, '')
),
emp_bco AS (
	SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY employer_name, cnpj) AS RN
	FROM emp_bco_join
)
SELECT
	rec.ano,
	rec.trimestre,
	rec.categoria,
	rec.tipo,
	rec.cnpj,
	rec.instituicao_financeira,
	rec.indice,
	rec.qtd_reclamacoes_reguladas_procedentes,
	rec.qtd_reclamacoes_reguladas_outras,
	rec.qtd_reclamacoes_nao_reguladas,
	rec.qtd_total_clientes_ccs_e_scr,
	rec.qtd_clientes_ccs,
	rec.qtd_clientes_scr,
	rec.qtd_total_reclamacoes,
	gdoor.employer_name,
	gdoor.reviews_count,
	gdoor.culture_count,
	gdoor.salaries_count,
	gdoor.benefits_count,
	gdoor.employerwebsite,
	gdoor.employerheadquarters,
	gdoor.employerfounded,
	gdoor.employerindustry,
	gdoor.employerrevenue,
	gdoor.url,
	gdoor.geral,
	gdoor.cultura_e_valores,
	gdoor.diversidade_e_inclusao,
	gdoor.qualidade_de_vida,
	gdoor.alta_lideranca,
	gdoor.remuneracao_e_beneficios,
	gdoor.oportunidades_de_carreira,
	gdoor.recomendam_para_outras_pessoas,
	gdoor.perspectiva_positiva_da_empresa,
	gdoor.segmento,
	gdoor.nome
FROM emp_bco AS gdoor
INNER JOIN rereclamacoes_agg AS rec
	ON gdoor.cnpj = rec.cnpj
	OR gdoor.nome = rec.instituicao_financeira
WHERE gdoor.RN = 1
ORDER BY instituicao_financeira, ano, trimestre;