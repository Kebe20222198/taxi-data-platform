-- ==============================================================================
-- Dimension: dim_payment_type
-- Grain: 1 payment method (1 row per TLC payment type code)
-- Kimball: Conformed Payment Type Dimension
-- ==============================================================================

{{
  config(
    materialized = 'table'
  )
}}

WITH payment_types AS (
    SELECT 1 AS payment_type_id, 'CREDIT_CARD' AS payment_code, 'Carte bancaire' AS payment_type_name, 'Règlement par carte bancaire ou paiement électronique' AS description, TRUE AS is_electronic, FALSE AS is_cash UNION ALL
    SELECT 2 AS payment_type_id, 'CASH' AS payment_code, 'Espèces' AS payment_type_name, 'Règlement direct en espèces auprès du chauffeur' AS description, FALSE AS is_electronic, TRUE AS is_cash UNION ALL
    SELECT 3 AS payment_type_id, 'NO_CHARGE' AS payment_code, 'Sans frais' AS payment_type_name, 'Course offerte ou sans facturation' AS description, FALSE AS is_electronic, FALSE AS is_cash UNION ALL
    SELECT 4 AS payment_type_id, 'DISPUTE' AS payment_code, 'Litige' AS payment_type_name, 'Course contestée par le passager ou incident de paiement' AS description, FALSE AS is_electronic, FALSE AS is_cash UNION ALL
    SELECT 5 AS payment_type_id, 'UNKNOWN' AS payment_code, 'Inconnu' AS payment_type_name, 'Mode de paiement non renseigné ou non identifié' AS description, FALSE AS is_electronic, FALSE AS is_cash UNION ALL
    SELECT 6 AS payment_type_id, 'VOIDED_TRIP' AS payment_code, 'Annulé' AS payment_type_name, 'Course annulée après enclenchement du compteur' AS description, FALSE AS is_electronic, FALSE AS is_cash UNION ALL
    SELECT 0 AS payment_type_id, 'UNKNOWN_ZERO' AS payment_code, 'Inconnu' AS payment_type_name, 'Code zéro non documenté' AS description, FALSE AS is_electronic, FALSE AS is_cash
)

SELECT
    payment_type_id,
    payment_code,
    payment_type_name,
    description,
    is_electronic,
    is_cash
FROM payment_types
