
-- Use the `ref` function to select from other models

select MD5(CONCAT_WS('|',ProductID,ProductCategory,ProductBrand)),ProductID,ProductCategory,ProductBrand,ProductPrice,CustomerAge,CustomerGender,PurchaseFrequency,CustomerSatisfaction,PurchaseIntent,count(*)
from {{ ref('consumer_data') }}
group by ALL

-- describe {{ ref('consumer_data') }}