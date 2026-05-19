<template>
  <v-card class="min-w-3xs max-w-2xl flex-1">
    <v-card-title>Journals' Countries</v-card-title>

    <v-card-text>
      <v-tabs v-model="tab" color="primary" grow>
        <v-tab value="one">Map</v-tab>
        <v-tab value="two">Bar Chart</v-tab>
      </v-tabs>

      <v-divider />

      <v-tabs-window v-model="tab">
        <v-tabs-window-item value="one">
          <country-world-map :data="props.data" data-key="country_code" data-value="count" />
        </v-tabs-window-item>

        <v-tabs-window-item value="two">
          <bar-chart
            :data="props.data.slice(0, maxCountryBar)"
            data-key="Country"
            data-value="count"
            :height="400"
          />

          <v-slider
            v-model="maxCountryBar"
            :label="`Show top ${maxCountryBar} countries`"
            :max="100"
            :min="10"
            :step="1"
          />
        </v-tabs-window-item>
      </v-tabs-window>
    </v-card-text>
  </v-card>
</template>
<script setup>
  import { ref } from 'vue'
  import BarChart from '@/components/charts/BarChart.vue'
  import CountryWorldMap from '@/components/charts/CountryWorldMap.vue'

  const tab = ref('one')
  const maxCountryBar = ref(30)

  const props = defineProps({
    data: {
      type: Array,
      required: true
    },
  })
</script>
