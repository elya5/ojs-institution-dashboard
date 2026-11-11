<template>
  <v-container class="mt-10" max-width="900">
    <div class="flex">
      <h1 class="text-4xl">Country Overview</h1>

      <v-progress-circular
        v-if="loading"
        class="ms-10"
        color="primary"
        indeterminate
        :size="50"
      />
    </div>

    <v-select
      v-model="selectedCountry"
      class="mt-10"
      item-title="name"
      item-value="key"
      :items="countries"
      label="Select"
      variant="outlined"
    />
  </v-container>

  <v-container>
    <div v-if="!loading && countryData" class="flex flex-wrap gap-5">
      <yearly-stats-tile :data="countryData.yearly_articles" />

      <disciplines-tile :data="countryData.disciplines" />

      <author-institution-tile :data="countryData.institutions" />

      <top-authors-tile :data="countryData.top_authors" />

      <author-countries-tile :data="countryData.countries" />

      <journal-countries-tile :data="countryData.journal_locations" />
    </div>
  </v-container>
</template>

<script setup>
  import { onMounted, ref, watch } from 'vue'
  import { apiGet } from '@/api'
  import AuthorCountriesTile from '@/components/AuthorCountriesTile.vue'
  import AuthorInstitutionTile from '@/components/AuthorInstitutionTile.vue'
  import DisciplinesTile from '@/components/DisciplinesTile.vue'
  import JournalCountriesTile from '@/components/JournalCountriesTile.vue'
  import TopAuthorsTile from '@/components/TopAuthorsTile.vue'
  import YearlyStatsTile from '@/components/YearlyStatsTile.vue'

  const countries = ref([])
  const loading = ref(false)
  const selectedCountry = ref(null)
  const countryData = ref(null)

  onMounted(async () => {
    countries.value = await apiGet('/list-countries')
  })

  watch(selectedCountry, fetchData)

  async function fetchData () {
    loading.value = true

    countryData.value = await apiGet('/country-analysis/' + selectedCountry.value)

    loading.value = false
  }
</script>
