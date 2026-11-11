<template>
  <VChart
    autoresize
    class="chart"
    :option="option"
    :style="{ height: height + 'px' }"
  />
</template>

<script setup>
  import { PieChart } from 'echarts/charts'
  import { LegendComponent, TitleComponent, TooltipComponent } from 'echarts/components'
  import { use } from 'echarts/core'
  import { CanvasRenderer } from 'echarts/renderers'
  import { computed } from 'vue'
  import VChart from 'vue-echarts'

  const props = defineProps({
    data: {
      type: Array,
      required: true,
    },
    dataKey: {
      type: String,
      required: true,
    },
    dataValue: {
      type: String,
      required: true,
    },
    height: {
      type: Number,
      default: 400,
    },
  })

  use([PieChart, CanvasRenderer, TooltipComponent, LegendComponent, TitleComponent])

  const option = computed(() => ({
    tooltip: {
      trigger: 'item',
      formatter: '{b}: {c} ({d}%)',
    },
    legend: {
      orient: 'vertical',
      left: 'left',
      data: props.data.map(d => d[props.dataKey]),
    },
    series: [
      {
        color: ['#fbb369', '#9f0712'],
        type: 'pie',
        radius: '50%',
        data: props.data.map(d => ({
          name: d[props.dataKey],
          value: d[props.dataValue],
        })),
        emphasis: {
          itemStyle: {
            shadowBlur: 10,
            shadowOffsetX: 0,
            shadowColor: 'rgba(0, 0, 0, 0.5)',
          },
        },
      },
    ],
  }))
</script>
