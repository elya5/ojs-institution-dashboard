<template>
  <VChart
  autoresize
    class="chart"
    :option="option"
    :style="{ height: height + 'px' }"
  />
</template>

<script setup>
  import { LineChart } from 'echarts/charts'
  import {
    GridComponent,
    LegendComponent,
    TitleComponent,
    TooltipComponent,
  } from 'echarts/components'
  import { use } from 'echarts/core'
  import { CanvasRenderer } from 'echarts/renderers'
  import { computed } from 'vue'
  import VChart from 'vue-echarts'

  use([
    LineChart,
    CanvasRenderer,
    GridComponent,
    TitleComponent,
    TooltipComponent,
    LegendComponent,
  ])

  const props = defineProps({
    data: Array,
    dataKey: String,
    dataValue: String,
    dataDimension: String,
    height: {
      type: Number,
      default: 400,
    },
  })

  const option = computed(() => {
    const xvalues = [...new Set(props.data.map(d => d[props.dataKey]))]
    const dimensions = [...new Set(props.data.map(d => d[props.dataDimension]))]

    const series = dimensions.map(dimension => {
      const rows = props.data.filter(d => d[props.dataDimension] === dimension)

      const data = xvalues.map(category => {
        const row = rows.find(r => r[props.dataKey] === category)
        return row ? row[props.dataValue] : null
      })

      return {
        name: dimension,
        type: 'line',
        smooth: true,
        data,
        symbol: 'circle',
        symbolSize: 7,
        lineStyle: {
          width: 3,
        },
      }
    })

    return {
      tooltip: {
        trigger: 'axis',
      },
      legend: {
        top: 0,
      },
      grid: {
        left: 50,
        right: 20,
        top: 50,
        bottom: 80,
      },
      xAxis: {
        type: 'category',
        name: props.dataKey,
        nameLocation: 'middle',
        nameGap: 60,
        axisLabel: {
          interval: 0,
          rotate: -45,
        },
        data: xvalues,
      },
      yAxis: {
        type: 'value',
      },
      series,
    }
  })
</script>
