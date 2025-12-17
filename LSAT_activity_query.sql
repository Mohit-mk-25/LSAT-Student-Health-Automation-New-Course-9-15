WITH
lsat_enr AS (
  select
  perd.person_first_name
  	, perd.person_last_name 
  	, perd.person_email 
    , ph.id AS kbs_enrollment_id
    , perd.person_student_id
    , ph.created_on AS enroll_start_date
    , ph.expiration_date AS enroll_exp_date
    , prod.product_code 
    , cl.code AS class_code
    , cl.start_date as class_start_date

  FROM kbs_billing.purchase_history ph
  INNER JOIN kaptest_common.course_access ca
    ON ph.id = ca.enrollment_id
  INNER JOIN bi_reporting.vw_product_detail prod
    ON ph.product_id = prod.product_id
  Left join bi_reporting.person_detail perd
  	on ph.person_id = perd.person_id 
   LEFT JOIN
    kbs_ess.class cl ON ph.class_id = cl.id
  WHERE
    ph.status NOT IN ('Drop', 'Freeze')
     AND ph.id NOT IN (
      SELECT transaction.purchase_id FROM kbs_so.transaction
      WHERE transaction.code = '476'
    )
    AND  (prod.product_code in ({lol_product_placeholder})
		or prod.product_code in ({tut_product_placeholder})
		)
	and ph.start_date >= ({ESD_placeholder_start})
  and ph.start_date <= ({ESD_placeholder_end})
  and (
       perd.person_email not like '%mailinator.com%'
    or perd.person_email not like '%knagrad.com%'
  )
)

select
	lsat_enr.person_first_name 
  , lsat_enr.person_last_name 
  , lsat_enr.person_email 
  ,	lsat_enr.kbs_enrollment_id 
  , lsat_enr.person_student_id 
  , lsat_enr.product_code
  , lsat_enr.class_code
  , lsat_enr.class_start_date
  , lsat_enr.enroll_start_date
  , lsat_enr.enroll_exp_date
  , actf.date_created
  , actf.date_completed
  , actf.status
  , actf.activity_name AS sequence_name
  , actf.activity_title AS sequence_title
  , actf.activity_type
  , actf.activity_subtype
  , actf.total_items
  , actf.total_scored_items
  , actf.total_scored_items_answered
  , actf.total_scored_items_answered_correct
  , actf.activity_id AS sequence_id
  , actf.total_scored_items_time_elapsed

--count(*)
FROM lsat_enr
 left JOIN kna.student_performance.activity_fact actf
  on lsat_enr.kbs_enrollment_id = actf.business_enrollment_id 
  and actf.status = 'completed'