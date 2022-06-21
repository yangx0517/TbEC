error=0
error_array=()
num=$3
flag=$1 
bf=$2 
start_time=0
for ((i=1; i<=$num; i++))
do
    echo "--------$i----------"
    start_time=`date +%s`
    ssh pi@192.168.1.114 "cd /home/pi/yangxu/triangle; python TbEC_collector_test.py dataset/Email-Enron_reverse_0 " &
    ssh pi@192.168.1.115 "cd /home/pi/yangxu/triangle; python TbEC_collector_test.py dataset/Email-Enron_reverse_1 " &
    ssh pi@192.168.1.116 "cd /home/pi/yangxu/triangle; python TbEC_collector_test.py dataset/Email-Enron_reverse_2 " &
    ssh pi@192.168.1.117 "cd /home/pi/yangxu/triangle; python TbEC_collector_test.py dataset/Email-Enron_reverse_3" &

    python TbEC/TbEC_master_10.py & 
    python TbEC/TbEC_worker.py 10 0 &  
    python TbEC/TbEC_worker.py 10 1 & 
    python TbEC/TbEC_worker.py 10 2 &
    python TbEC/TbEC_worker.py 10 3 &  
    python TbEC/TbEC_worker.py 10 4 & 
    python TbEC/TbEC_worker.py 10 5 &
    python TbEC/TbEC_worker.py 10 6 &  
    python TbEC/TbEC_worker.py 10 7 & 
    python TbEC/TbEC_worker.py 10 8 &
    python TbEC/TbEC_worker.py 10 9

    res=$(python TbEC/TbEC_aggregator.py 10)

    end_time=`date +%s`
    dur_time=`expr $end_time - $start_time`
    echo $res
    error_array[${#error_array[*]}]=$res
    error=`echo "$error + $res"|bc`
    echo "Time: $dur_time s"
    time_cost=`expr $time_cost + $dur_time`

    echo "--------Result----------"
    this_error=`echo "scale=8;$error/$i"|bc`
    echo "Average Error: $this_error"
    this_time_cost=`echo "scale=8;$time_cost/$i"|bc`
    echo "Average Time: $this_time_cost s"

    wait
done

#  error_array排序  

for ((a=0;a<${#error_array[*]};a++))
do
    for ((k=$a+1;k<${#error_array[*]};k++))
    do
        if [ `echo "${error_array[$a]} > ${error_array[$k]}" | bc` -eq 1 ]; then
            qq=${error_array[$a]}
            error_array[$a]=${error_array[$k]}
            error_array[$k]=$qq
        fi
    done
done
len=${#error_array[*]}

