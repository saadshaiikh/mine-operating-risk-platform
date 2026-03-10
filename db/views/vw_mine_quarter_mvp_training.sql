CREATE OR REPLACE VIEW vw_mine_quarter_mvp_training AS
SELECT *
FROM vw_mine_quarter_mvp_features
WHERE had_incident_next_qtr IS NOT NULL;
