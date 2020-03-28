var statsChart = echarts.init(document.getElementById('stats'));
var statsOption = {
    title: {
        text: '增长',
        left:50
    },
    tooltip: {
        trigger: 'axis'
    },
    legend: {
        data:['新用户','总用户','订单','GMV']
    },
    grid: {
        left: '3%',
        right: '4%',
        bottom: '3%',
        containLabel: true
    },
    xAxis: {
        type: 'category',
        boundaryGap: false,
        data: ['周一','周二','周三','周四','周五','周六','周日']
    },
    yAxis: {
        type: 'value'
    },
    series: [
        {
            name:'新用户',
            type:'line',
            stack: '总量',
            data:[120, 132, 101, 134, 90, 230, 210]
        },
        {
            name:'总用户',
            type:'line',
            stack: '总量',
            data:[220, 182, 191, 234, 290, 330, 310]
        },
        {
            name:'订单',
            type:'line',
            stack: '总量',
            data:[150, 232, 201, 154, 190, 330, 410]
        },
        {
            name:'GMV',
            type:'line',
            stack: '总量',
            data:[320, 332, 301, 334, 390, 330, 320]
        }
    ]
};

statsChart.setOption(statsOption);