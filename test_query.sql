select distinct actf.activity_name,actf.activity_title 
from kna.student_performance.activity_fact actf
where 
actf.activity_name  in ({act_name_placeholder})
and actf.date_created >= '2024-01-01' 