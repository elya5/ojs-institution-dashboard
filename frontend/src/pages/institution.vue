<template>
  <v-container class="mt-10" max-width="900">
    <div class="flex">
      <h1 class="text-4xl">Institution Overview</h1>

      <v-progress-circular
        v-if="loadingResults"
        class="ms-10 mt-5"
        color="primary"
        indeterminate
        :size="50"
      />
    </div>

    <v-autocomplete
      v-model="selectedInstitution"
      v-model:menu="institutionsMenu"
      v-model:search="searchInstitutions"
      autocomplete="off"
      class="mt-10"
      hide-details
      hide-no-data
      item-title="name"
      item-value="ror"
      :items="institutionsSuggestions"
      label="Institution"
      :loading="loadingInstitutions"
      placeholder="Type name of institution..."
      variant="outlined"
      @update:model-value="analyseInstitute"
    />

    <div v-if="!loadingResults && data" class="mt-15">
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

      <v-card v-if="data.ojs_share" class="bg-white border-gray-200 m-2" variant="outlined">
        <v-card-title>Share of OJS Articles</v-card-title>

        <v-card-text>
          <pie-chart
            :data="data.ojs_share"
            data-key="Publisher Software"
            data-value="Count"
            :height="300"
          />
        </v-card-text>
      </v-card>

      <v-card class="bg-white border-gray-200 m-2 mt-4" variant="outlined">
        <v-card-title>Disciplines</v-card-title>

        <v-card-text>
          <bar-chart
            :data="data.disciplines"
            data-dimension="Type"
            data-key="Field"
            data-value="Count"
            :height="400"
          />
        </v-card-text>
      </v-card>

      <v-card class="bg-white border-gray-200 m-2 mt-4" variant="outlined">
        <v-card-title>Journal Countries</v-card-title>

        <v-card-text>
          <country-world-map :data="data.journal_locations" data-key="journal_country_code" data-value="count" />
        </v-card-text>
      </v-card>

      <v-card class="bg-white border-gray-200 m-2 mt-4" variant="outlined">
        <v-card-title>Languages</v-card-title>

        <v-card-text>
          <bar-chart
            :data="data.languages"
            data-key="language"
            data-value="Count"
            :height="300"
          />
        </v-card-text>
      </v-card>

      <v-card class="bg-white border-gray-200 m-2 mt-4" variant="outlined">
        <v-card-title>Collaborations</v-card-title>

        <v-card-text>
          <v-tabs v-model="collaborationTab" fixed-tabs>
            <v-tab value="one">Countries</v-tab>
            <v-tab value="two">Institutions</v-tab>
          </v-tabs>

          <v-divider />

          <v-tabs-window v-model="collaborationTab">
            <v-tabs-window-item value="one">
              <v-sheet class="p-2">
                <country-world-map
                  :data="data.countries"
                  data-key="country_code"
                  data-value="count"
                />
              </v-sheet>
            </v-tabs-window-item>

            <v-tabs-window-item value="two">
              <v-sheet class="p-2">
                <span class="text-xs text-gray-500">The top 500 collaborating institutions are shown.</span>

                <scatter-world-map
                  :data="data.institutions.toSorted((a, b) => b.weight - a.weight).slice(0, 500)"
                  data-key="Institution"
                  data-value="weight"
                />
              </v-sheet>
            </v-tabs-window-item>
          </v-tabs-window>
        </v-card-text>
      </v-card>
    </div>
  </v-container>
</template>
<script setup>
  import { ref, watch } from 'vue'
  import { apiGet } from '@/api'
  import BarChart from '@/components/charts/BarChart.vue'
  import CountryWorldMap from '@/components/charts/CountryWorldMap.vue'
  import LineChart from '@/components/charts/LineChart.vue'
  import PieChart from '@/components/charts/PieChart.vue'
  import ScatterWorldMap from '@/components/charts/ScatterWorldMap.vue'

  // settings and selection
  const fractional = ref(false)
  const loadingResults = ref(false)
  const selectedInstitution = ref(null)
  const searchInstitutions = ref(null)
  const institutionsSuggestions = ref([])
  const loadingInstitutions = ref(false)
  const institutionsMenu = ref(false)

  // results
  const data = ref(null)
  const collaborationTab = ref('one')

  watch(searchInstitutions, val => {
    if (val) {
      const selectedName = institutionsSuggestions.value.find(i => i.ror === selectedInstitution.value)?.name
      val !== selectedName && querySelections(val)
    }
  })

  watch(fractional, () => analyseInstitute())

  function querySelections (v) {
    loadingInstitutions.value = true
    setTimeout(async () => {
      if (v !== searchInstitutions.value) {
        return
      }
      institutionsSuggestions.value = await apiGet('/institutions/suggest?q=' + v)
      institutionsMenu.value = institutionsSuggestions.value.length > 0
      loadingInstitutions.value = false
    }, 2000)
  }

  async function analyseInstitute () {
    data.value = null
    loadingResults.value = true

    const params = new URLSearchParams({
      fractional: fractional.value,
      institution: selectedInstitution.value,
    })
    data.value = await apiGet('/institution-analysis/?' + params.toString())

    loadingResults.value = false
  }
</script>
