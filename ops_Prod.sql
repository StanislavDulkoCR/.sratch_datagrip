select count(*)
from prod.model_score ms
where ms.create_feed_instance_id = '9AC20678-A59A-4011-BC3B-4FA15D5A7A9D14240404095033'
limit 100;

select max(create_date)
from prod.model_score
limit 100;