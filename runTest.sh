echo "Testing ..."
csh qutest 1 > output1.txt
csh qutest 5 > output5.txt
csh qutest 7 > output7.txt
diff output1.txt test1.txt -w -B --strip-trailing-cr
diff output5.txt test5.txt -w -B --strip-trailing-cr 
diff output7.txt test7.txt -w -B --strip-trailing-cr
echo "End test"
