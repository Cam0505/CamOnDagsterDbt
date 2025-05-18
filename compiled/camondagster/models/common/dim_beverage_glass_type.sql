SELECT glass_type, glass_type_sk
    from "camondagster"."public_snapshots"."glass_type_snapshot"
	group by glass_type, glass_type_sk