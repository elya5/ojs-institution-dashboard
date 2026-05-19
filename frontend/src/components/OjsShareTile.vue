<template>
  <v-card class="min-w-150 max-w-2xl flex-1">
    <v-card-title>Share of OJS Articles</v-card-title>

    <v-card-text>
      <pie-chart
        :data="props.data"
        data-key="Publisher Software"
        data-value="Count"
        :height="400"
      />
    </v-card-text>
</v-card>
</template>

<script setup>
  import PieChart from '@/components/charts/PieChart.vue'

  const props = defineProps({
    data: {
      type: Array,
      required: true
    },
  })
</script>
