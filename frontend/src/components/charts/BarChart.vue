<template>
  <VChart autoresize class="chart" :option="option" :style="{'height': height + 'px'}" />
</template>
<script setup>
  import { BarChart } from 'echarts/charts'
  import { GridComponent, LegendComponent, TitleComponent, TooltipComponent } from 'echarts/components'
  import { use } from 'echarts/core'
  import { CanvasRenderer } from 'echarts/renderers'
  import { computed } from 'vue'
  import VChart from 'vue-echarts'


  const props = defineProps({
    data: Object,
    dataDimension: String,
    dataKey: String,
    dataValue: String,
    height: Number
  })

  use([BarChart, CanvasRenderer, GridComponent, TitleComponent, TooltipComponent, LegendComponent])

  const option = computed(() => {
    const xvalues = [...new Set(props.data.map(d => d[props.dataKey]))]

    const dimensions = props.dataDimension
      ? [...new Set(props.data.map(d => d[props.dataDimension]))]
      : ['']

    const series = dimensions.map(dim => {
      return {
        name: dim,
        type: 'bar',
        data: xvalues.map(xvalue => {
          const row = props.data.find(d =>
            d[props.dataKey] === xvalue
            && (!props.dataDimension || d[props.dataDimension] === dim)
          )
          return row ? row[props.dataValue] : 0
        })
      }
    })
    return {
      tooltip: {
        trigger: 'axis',
        axisPointer: {
          type: 'shadow'
        },
      },
      legend: {
        show: props.dataDimension && dimensions.length > 1
      },
      grid: {
        left: '3%',
        right: '4%',
        bottom: '15%',
        containLabel: true
      },
      xAxis: {
        type: 'category',
        name: props.dataKey,
        nameLocation: 'middle',
        nameGap: 10,
        axisLabel: {
          interval: 0,
          rotate: -45
        },
        data: xvalues
      },
      yAxis: {},
      series
    }
  })
</script>
