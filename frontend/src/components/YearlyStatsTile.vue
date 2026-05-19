<template>
  <v-card class="min-w-150 max-w-2xl flex-1">
    <v-card-title>Number of Articles</v-card-title>

    <v-card-text>
      <line-chart
        :data="props.data"
        data-dimension="Dimension"
        data-key="Publication Year"
        data-value="Count"
        :height="400"
      />
    </v-card-text>
  </v-card>
</template>

<script setup>
  import LineChart from '@/components/charts/LineChart.vue'

  const props = defineProps({
    data: {
      type: Array,
      required: true
    },
  })
</script>
