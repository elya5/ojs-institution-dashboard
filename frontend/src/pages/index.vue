<template>
  <v-container class="mt-10" max-width="900">
    <div>Open Journal Systems (OJS) is a free, open-source platform developed by the Public Knowledge Project (PKP) that powers scholarly journal publishing from submission through peer review to online publication.</div>

    <div class="mt-10 flex flex-wrap justify-between">
      <v-card class="bg-gray-50 min-w-3xs m-2" title="OJS Journals">
        <template #text>
          <span class="text-5xl">{{ !stats ? '…' : stats.journals.toLocaleString() }}</span>
        </template>
      </v-card>

      <v-card class="bg-gray-50 min-w-2xs m-2" title="OJS Articles">
        <template #text>
          <p class="text-5xl mb-2">{{ !stats ? '…' : stats.articles.toLocaleString() }}</p>
          <p>since 2010</p>
        </template>
      </v-card>

      <v-card class="bg-gray-50 min-w-3xs m-2" title="Hosting Countries">
        <template #text>
          <p class="text-5xl">{{ !stats ? '…' : stats.countries }}</p>
        </template>
      </v-card>
    </div>

    <VChart class="chart mt-10" :option="option" />
  </v-container>
</template>

<script setup>
  import { LineChart } from 'echarts/charts'
  import { GridComponent, TitleComponent, TooltipComponent } from 'echarts/components'
  import { use } from 'echarts/core'
  import { CanvasRenderer } from 'echarts/renderers'
  import { onMounted, ref } from 'vue'
  import VChart from 'vue-echarts'

  const stats = ref(null)
  use([CanvasRenderer, GridComponent, LineChart, TitleComponent, TooltipComponent])

  const option = ref({
    title: {
      text: 'Number of OJS Articles',
      left: 'left',
    },
    tooltip: {
      trigger: 'item',
      formatter: '{a} <br/>{b} : {c}',
    },
    xAxis: {
      type: 'category',
      name: 'Year',
      nameLocation: 'middle',
      nameGap: 50,
      data: [],
    },
    yAxis: {
      type: 'value',
      name: 'Number of Articles',
      nameLocation: 'middle',
      nameGap: 50,
      nameRotate: 90,
    },
    series: [
      {
        data: [],
        type: 'line',
        name: 'OJS Articles'
      },
    ],
  })

  onMounted(async () => {
    const res_s = await fetch('http://localhost:8000/get-ojs-journals-stats')
    const data_s = await res_s.json()
    stats.value = data_s

    const res_y = await fetch('http://localhost:8000/all-articles-yearly')
    const data_y = await res_y.json()
    option.value.xAxis.data = data_y.map(d => d.Year)
    option.value.series[0].data = data_y.map(d => d.count)
  })
</script>

<style scoped>
.chart {
  height: 400px;
}
</style>
