<template>
  <v-container class="mt-10" max-width="900">
    <div class="flex">
      <h1 class="text-4xl">Institution Overview</h1>

      <v-progress-circular
        v-if="loadingResults"
        class="ms-10"
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
      @update:model-value="analyseInstitute"
    />

    <v-expansion-panels class="mt-5">
      <v-expansion-panel title="Options">
        <v-expansion-panel-text>
          <v-switch v-model="fractional" label="Fractional Counting" />
        </v-expansion-panel-text>
      </v-expansion-panel>
    </v-expansion-panels>
  </v-container>

  <v-container>
    <div v-if="!loadingResults && data" class="flex flex-wrap gap-5">
      <top-authors-tile :data="data.top_authors" />

      <journal-countries-tile :data="data.journal_locations" />

      <disciplines-tile :data="data.disciplines" />

      <ojs-share-tile v-if="data.ojs_share" :data="data.ojs_share" />

      <yearly-stats-tile :data="data.yearly_stats" />

      <author-institution-tile :data="data.institutions" />
    </div>
  </v-container>
</template>
<script setup>
  import { ref, watch } from 'vue'
  import AuthorInstitutionTile from '@/components/AuthorInstitutionTile.vue'
  import DisciplinesTile from '@/components/DisciplinesTile.vue'
  import JournalCountriesTile from '@/components/JournalCountriesTile.vue'
  import OjsShareTile from '@/components/OjsShareTile.vue'
  import TopAuthorsTile from '@/components/TopAuthorsTile.vue'
  import YearlyStatsTile from '@/components/YearlyStatsTile.vue'

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
  const journalcountryTab = ref('one')
  const authorinstitutionTab = ref('one')

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
      const res = await fetch('http://localhost:8000/institutions/suggest?q=' + v)
      institutionsSuggestions.value = await res.json()
      institutionsMenu.value = institutionsSuggestions.value.length > 0
      loadingInstitutions.value = false
    }, 2000)
  }

  async function analyseInstitute () {
    data.value = null
    loadingResults.value = true

    const params = new URLSearchParams({
      fractional: fractional.value,
      institution: selectedInstitution.value
    })
    const res = await fetch(
      'http://localhost:8000/institution-analysis/?' + params.toString()
    )
    data.value = await res.json()

    loadingResults.value = false
  }
</script>
