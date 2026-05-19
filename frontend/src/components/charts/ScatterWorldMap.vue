<template>
  <VChart
    autoresize
    class="chart"
    :option="option"
    :style="{ height: height + 'px' }"
  />
</template>

<script setup>
  import * as echarts from 'echarts'
  import { ScatterChart } from 'echarts/charts'
  import {
    GeoComponent,
    GridComponent,
    TitleComponent,
    TooltipComponent,
    VisualMapComponent,
  } from 'echarts/components'
  import { use } from 'echarts/core'
  import { CanvasRenderer } from 'echarts/renderers'
  import { computed, onMounted, ref } from 'vue'
  import VChart from 'vue-echarts'

  use([
    CanvasRenderer,
    ScatterChart,
    GeoComponent,
    GridComponent,
    TitleComponent,
    TooltipComponent,
    VisualMapComponent,
  ])

  const props = defineProps({
    data: {
      type: Array,
      required: true,
    },
    height: {
      type: Number,
      default: 500,
    },
    dataKey: String,
    dataValue: String
  })

  const mapReady = ref(false)

  onMounted(async () => {
    const resp = await fetch('/world.json')
    const world = await resp.json()
    echarts.registerMap('world', world)
    mapReady.value = true
  })

  const option = computed(() => {
    if (!mapReady.value) return {}

    const values = props.data.map(d => d[props.dataValue])

    return {
      tooltip: {
        trigger: 'item',
        formatter: p => {
          return `
            ${p.value[3]}<br/>
            Value: ${p.value[2]}
          `
        },
      },
      geo: {
        map: 'world',
        roam: true,
        silent: true,
        scaleLimit: {
          min: 1,
          max: 20,
        },
        itemStyle: {
          areaColor: '#e5e7eb',
          borderColor: '#9ca3af',
        },
        emphasis: {
          itemStyle: {
            areaColor: '#d1d5db',
          },
        },
      },
      visualMap: {
        min: 0,
        max: Math.max(...values, 1),
        calculable: true,
        orient: 'vertical',
        left: 20,
        bottom: 20,
        text: ['High', 'Low'],
        dimension: 2,
        inRange: {
          symbolSize: [4, 25],
        },
        controller: {
          inRange: {
            symbolSize: [4, 25],
            color: ['#93c5fd', '#2563eb'],
          },
        },
      },
      series: [
        {
          type: 'scatter',
          coordinateSystem: 'geo',
          geoIndex: 0,
          data: props.data.map((d) => [
            d.lng,
            d.lat,
            d[props.dataValue],
            d[props.dataKey],
          ]),
          emphasis: {
            focus: 'series',
            label: {
              show: true,
              formatter: param => param.data[3],
            },
          },
          itemStyle: {
            color: '#3b82f6',
          },
        },
      ],
    }
  })
</script>
