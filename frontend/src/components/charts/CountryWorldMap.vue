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
  import { MapChart } from 'echarts/charts'
  import {
    GeoComponent,
    TooltipComponent,
    VisualMapComponent,
  } from 'echarts/components'
  import { use } from 'echarts/core'
  import { CanvasRenderer } from 'echarts/renderers'
  import { computed, onMounted, ref } from 'vue'
  import VChart from 'vue-echarts'

  use([
    CanvasRenderer,
    MapChart,
    GeoComponent,
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
    dataValue: String,
  })

  const mapReady = ref(false)
  const world = ref({})

  onMounted(async () => {
    const resp = await fetch('/world.json')
    world.value = await resp.json()

    echarts.registerMap('world', world.value)
    mapReady.value = true
  })

  const option = computed(() => {
    if (!mapReady.value) return {}

    const countryNameMap = new Map(
      world.value.features.map(f => ([f.properties.country_code, f.properties.name]))
    )
    const mapData = props.data.map(d => ({
      name: countryNameMap.get(d[props.dataKey]),
      value: d[props.dataValue],
    }))

    const values = mapData.map(d => d.value)

    return {
      tooltip: {
        trigger: 'item',
        formatter: params => {
          if (params.value == null) {
            return `${params.name}<br/>No data`
          }

          return `
            ${params.name}<br/>
            Value: ${params.value || 0}
          `
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
        inRange: {
          color: ['#bfdbfe', '#2563eb'],
        },
        outOfRange: {
          color: '#e5e7eb', // grey for no-data regions
        },
      },

      series: [
        {
          type: 'map',
          map: 'world',
          roam: true,
          scaleLimit: {
            min: 1,
            max: 20,
          },
          missingDataColor: '#000000',

          // match GeoJSON country property
          // change if your world.json uses a different field
          nameProperty: 'name', //'country_code',

          data: mapData,

          emphasis: {
            label: {
              show: true,
            },
            itemStyle: {
              areaColor: '#1d4ed8',
            },
          },

          itemStyle: {
            borderColor: '#9ca3af',
            areaColor: '#e5e7eb',
          },
        },
      ],
    }
  })
</script>
