<template>
  <v-container class="mt-10" max-width="900">
    <div class="flex">
      <h1 class="text-4xl">Journal Overview</h1>

      <v-progress-circular
        v-if="loading"
        class="ms-10"
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
      label="Journal"
      placeholder="Select journal..."
      @update:model-value="analyseJournal"
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
    <div v-if="!loading && data" class="flex flex-wrap gap-5">
      <yearly-stats-tile :data="data.yearly_stats" />

      <top-authors-tile :data="data.top_authors" />

      <author-countries-tile :data="data.author_countries" />

      <author-institution-tile :data="data.institutions" />
    </div>
  </v-container>
</template>
<script setup>
  import { onMounted, ref, watch } from 'vue'
  import AuthorCountriesTile from '@/components/AuthorCountriesTile.vue'
  import AuthorInstitutionTile from '@/components/AuthorInstitutionTile.vue'
  import TopAuthorsTile from '@/components/TopAuthorsTile.vue'
  import YearlyStatsTile from '@/components/YearlyStatsTile.vue'

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
      journalSuggestions.value = journals.value.filter(
        j => j.lowerName.length > vallow.length && j.lowerName.includes(vallow)
      ).slice(0, 200)
    }
  })

  watch(fractional, () => analyseJournal())

  onMounted(async () => {
    const res = await fetch('http://localhost:8000/journals')
    journals.value = await res.json()
    for (const j of journals.value) {
      j.lowerName = j.context_name.toLowerCase()
    }
  })

  async function analyseJournal () {
    if (!selectedJournal.value) {
      return
    }
    loading.value = true

    const params = new URLSearchParams({
      fractional: fractional.value,
      journal: selectedJournal.value
    })
    const res = await fetch('http://localhost:8000/journal-analysis/?' + params.toString())
    data.value = await res.json()

    loading.value = false
  }

</script>
