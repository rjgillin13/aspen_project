CREATE OR REPLACE PROCEDURE transform_to_base_tables() language plpgsql
AS $$
DECLARE cur_user varchar := (SELECT CURRENT_USER);
DECLARE cur_time timestamp := (Select current_timestamp);
Begin

INSERT INTO role_profile_type
(
    role_profile_type_id,
    type,
    created,
    created_by,
    updated,
    updated_by
)
(
SELECT
    row_number() OVER(order by role_profile) as role_profile_type_id,
    role_profile,
    cur_time,
    cur_user,
    cur_time,
    cur_user
    FROM
    (SELECT DISTINCT role_profile from stg_role_profile) t)
ON conflict ON CONSTRAINT role_profile_type_pkey
    DO UPDATE SET updated = cur_time + INTERVAL '1 DAY',
    updated_by = cur_user;

end
$$;