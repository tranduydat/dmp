export DMP_DS_PASSWORD='' # Password của username phía dưới
java -jar ./dmp-v1.jar \\ # Đường dẫn đến file .jar của DMP
  -h 116.103.227.204 \\ # Hostname/IP của SQL Server instance có chứa db cần lấy
  -p 8386 \\ # Port của SQL Server instance
  -u ptnt_group \\ # Username có quyền truy cập vào db
  -d dms \\ # Tên database
  -s ./dms_plan.txt \\ # Tên file plan, nên để dạng <tên db>_plan.txt


export DMP_DS_PASSWORD='' # Password của username phía dưới
java -jar ./dmp-v1.jar \\ # Đường dẫn đến file .jar của DMP
  -h 116.103.227.204 \\ # Hostname/IP của SQL Server instance có chứa db cần lấy
  -p 8386 \\ # Port của SQL Server instance
  -u ptnt_group \\ # Username có quyền truy cập vào db
  -d dms \\ # Tên database
  -s ./dms_plan.txt \\ # Tên file plan, nên để dạng <tên db>_plan.txt