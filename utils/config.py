
mongodb ={
    "CRAWLING":{
        "dbname"            :   "crawling", 
        "username"          :   "mongoadmin",
        "password"          :   "thanh123",
        "host"              :   "localhost",      
        "port"              :   8001  ,
        "cv_job_post_sitemap"    :   "cv_job_post_sitemap",
        "cv_job_post_detail"     :   "cv_job_post_detail",
        "cv_employer_sitemap":"cv_employer_sitemap",
        "cv_employer_detail" : "cv_employer_detail",
        "vnw_job_post_sitemap":"vnw_job_post_sitemap",
        "vnw_job_post_detail":"vnw_job_post_detail",
        "vnw_employer_sitemap":"vnw_employer_sitemap",
        "vnw_employer_detail":"vnw_employer_detail"
            
    } 
}

postgres ={
    "DWH":{
        "dbname"            :   "postgres", 
        "username"          :   "ETL",
        "password"          :   "thanh123",
        "host"              :   "localhost",      
        "port"              :   "8000",
        "cv_job_post_sitemap"    :   "stg.cv_job_post_sitemap",
        "cv_job_post_detail"     :   "stg.cv_job_post_detail",
        "cv_employer_sitemap":"stg.cv_employer_sitemap",
        "cv_employer_detail" : 'stg.cv_employer_detail',
        "vnw_job_post_sitemap":"stg.vnw_job_post_sitemap",
        "vnw_job_post_detail":"stg.vnw_job_post_detail",
        "vnw_employer_sitemap":"stg.vnw_employer_sitemap",
        "vnw_employer_detail":"stg.vnw_employer_detail"
    }
}