<template>
  <v-container class="mt-10" max-width="900">
    <div class="flex">
      <h1 class="text-4xl">Journal Overview</h1>

      <v-progress-circular
        v-if="loading"
        class="ms-10 mt-5"
        color="primary"
        indeterminate
        :size="50"
      />
    </div>

    <v-autocomplete
      v-model="selectedJournal"
      v-model:search="searchJournal"
      autocomplete="off"
      class="mt-10"
      hide-details
      hide-no-data
      item-title="context_name"
      item-value="issn"
      :items="journalSuggestions"
      label="Journal Name"
      placeholder="Type to start searching..."
      return-object
      variant="outlined"
      @update:model-value="analyseJournal"
    />

    <div v-if="!loading && data" class="mt-15">
      <v-card class="bg-white border-gray-200 m-2" variant="outlined">
        <v-card-title>Yearly Development</v-card-title>

        <v-card-text>
          <line-chart
            :data="data.yearly_stats"
            data-dimension="Dimension"
            data-key="Publication Year"
            data-value="Count"
            :height="400"
          />
        </v-card-text>
      </v-card>

      <v-card class="bg-white border-gray-200 m-2 mt-4" variant="outlined">
        <v-card-title>Author Countries</v-card-title>

        <v-card-text>
          <country-world-map
            :data="data.author_countries"
            data-key="country_code"
            data-value="count"
          />
        </v-card-text>
      </v-card>

      <v-card class="bg-white border-gray-200 m-2 mt-4" variant="outlined">
        <v-card-title>Author Institutions</v-card-title>

        <v-card-text>
          <span class="text-xs text-gray-500">The top 500 institutions are shown.</span>

          <scatter-world-map
            :data="data.institutions.toSorted((a, b) => b.weight - a.weight).slice(0, 500)"
            data-key="Institution"
            data-value="weight"
          />
        </v-card-text>
      </v-card>
    </div>
  </v-container>
</template>
<script setup>
  import { onMounted, ref, watch } from 'vue'
  import { apiGet } from '@/api'
  import CountryWorldMap from '@/components/charts/CountryWorldMap.vue'
  import LineChart from '@/components/charts/LineChart.vue'
  import ScatterWorldMap from '@/components/charts/ScatterWorldMap.vue'

  const selectedJournal = ref(null)
  const searchJournal = ref('')
  const journals = ref([])
  const journalSuggestions = ref([])
  const loading = ref(false)
  const data = ref(null)
  const fractional = ref(false)

  watch(searchJournal, val => {
    if (val && val.length > 3) {
      const vallow = val.toLowerCase()
      journalSuggestions.value = journals.value
        .filter(
          j =>
            j.lowerName.length > vallow.length && j.lowerName.includes(vallow),
        )
        .slice(0, 200)
    }
  })

  watch(fractional, () => analyseJournal())

  onMounted(async () => {
    journals.value = await apiGet('/journals')
    for (const j of journals.value) {
      j.lowerName = j.context_name.toLowerCase()
      if (j.context_name.length > 80) {
        j.context_name = j.context_name.slice(0, 80) + '…'
      }
    }
  })

  async function analyseJournal () {
    if (!selectedJournal.value.issn) {
      return
    }
    loading.value = true

    const params = new URLSearchParams({
      fractional: fractional.value,
      journal: selectedJournal.value.issn,
    })
    data.value = await apiGet('/journal-analysis/?' + params.toString())

    loading.value = false
  }
</script>
