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

  FROM kbs_billing.purchase_history ph
  INNER JOIN kaptest_common.course_access ca
    ON ph.id = ca.enrollment_id
  INNER JOIN bi_reporting.vw_product_detail prod
    ON ph.product_id = prod.product_id
  Left join bi_reporting.person_detail perd
  	on ph.person_id = perd.person_id 
  WHERE
  --  ph.status NOT IN ('Drop', 'Freeze')
  --   AND  ph.id NOT IN (
  --     SELECT transaction.purchase_id FROM kbs_so.transaction
  --     WHERE transaction.code = '476'
  --   )
    (prod.product_code in ({lol_product_placeholder})
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
 
      lsat_enr.kbs_enrollment_id
      ,lsat_enr.person_student_id
      , lsat_enr.enroll_start_date
      ,actf.activity_id                        
      ,actf.history_db_id                      
      ,actf.source_system                      
      ,actf.template_id                        
      ,actf.activity_name                      
      ,actf.activity_title                     
      ,actf.activity_type    
      ,actf.template_name                  
      ,actf.date_created                       
      ,actf.date_completed                     
      ,actf.status                             
      ,actf.potential_test_user  
      ,actf.total_items              
      ,actf.total_scored_items                 
      ,actf.total_scored_items_time_elapsed    
      ,actf.total_scored_items_answered        
      ,actf.total_scored_items_answered_correct 
      ,actf.total_scored_items_time_elapsed
      ,scf.score_name score_name
      ,scf.score_value score_value

FROM lsat_enr

 inner JOIN kna.student_performance.activity_fact actf
  on lsat_enr.kbs_enrollment_id = actf.business_enrollment_id 
  and actf.status = 'completed'
  and actf.source_system = 'Atom'
 JOIN kna.student_performance.score_fact scf 
	ON actf.activity_id = scf.activity_id
	AND actf.history_db_id = scf.history_db_id 
	AND scf.source_system = 'Atom'
-- where scf.score_name in ('scaledScore','3-section-scaled_scaled-score')
	

