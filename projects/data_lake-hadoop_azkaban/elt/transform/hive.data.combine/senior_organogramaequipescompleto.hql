CREATE VIEW staging.senior_organogramaequipescompleto AS
SELECT
	T1.idPosicaoEquipe				AS idEquipe,
	T1.idEquipe						AS nomeEquipe,
	T1.idPosicaoEquipePai			AS I_idEquipe,
	T1.idEquipePai  			    AS I_nomeEquipe,
	T2.idPosicaoEquipePai			AS II_idEquipe,
	T2.idEquipePai  			    AS II_nomeEquipe,
	T3.idPosicaoEquipePai			AS III_idEquipe,
	T3.idEquipePai  			    AS III_nomeEquipe,
	T4.idPosicaoEquipePai			AS IV_idEquipe,
	T4.idEquipePai  			    AS IV_nomeEquipe,
	T5.idPosicaoEquipePai			AS V_idEquipe,
	T5.idEquipePai  			    AS V_nomeEquipe,
	T6.idPosicaoEquipePai			AS VI_idEquipe,
	T6.idEquipePai  			    AS VI_nomeEquipe,
	T7.idPosicaoEquipePai			AS VII_idEquipe,
	T7.idEquipePai  			    AS VII_nomeEquipe
FROM staging.senior_hierarquiaequipe AS T1
LEFT JOIN staging.senior_hierarquiaequipe AS T2 ON T1.idPosicaoEquipePai = T2.idPosicaoEquipe
LEFT JOIN staging.senior_hierarquiaequipe AS T3 ON T2.idPosicaoEquipePai = T3.idPosicaoEquipe
LEFT JOIN staging.senior_hierarquiaequipe AS T4 ON T3.idPosicaoEquipePai = T4.idPosicaoEquipe
LEFT JOIN staging.senior_hierarquiaequipe AS T5 ON T4.idPosicaoEquipePai = T5.idPosicaoEquipe
LEFT JOIN staging.senior_hierarquiaequipe AS T6 ON T5.idPosicaoEquipePai = T6.idPosicaoEquipe
LEFT JOIN staging.senior_hierarquiaequipe AS T7 ON T6.idPosicaoEquipePai = T7.idPosicaoEquipe
SORT BY idEquipe DESC;
