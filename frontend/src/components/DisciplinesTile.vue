<template>
  <v-card class="min-w-150 max-w-2xl flex-1">
    <v-card-title>Disciplines</v-card-title>

    <v-card-text>
      <bar-chart
        :data="props.data"
        data-dimension="Type"
        data-key="Field"
        data-value="Count"
        :height="400"
      />
    </v-card-text>
  </v-card>
</template>
<script setup>
  import BarChart from '@/components/charts/BarChart.vue'

  const props = defineProps({
    data: {
      type: Array,
      required: true
    },
  })
</script>
