<template>
  <v-container class="mt-10" max-width="900">
    <div class="mb-5">
      Open Journal Systems (OJS) is a free, open-source platform developed by the
      Public Knowledge Project (PKP) that powers scholarly journal publishing from
      submission through peer review to online publication.
      <br />
      This dashboard presents information on OJS usage worldwide, highlighting the
      platform’s growing adoption across regions, disciplines, and publishing
      communities. It is intended to illustrate the increasing popularity and
      diversity of OJS and should not be used to compare the performance, quality,
      or impact of individual journals, publishers, or institutions.
    </div>

    <div class="mt-10 flex flex-wrap justify-between">
      <v-card class="bg-white border-gray-200 min-w-48 m-2 grow basis-0" title="OJS Journals" variant="outlined">
        <template #text>
          <span class="text-red-800 text-4xl font-semibold mt-0">{{ !data ? '…' : data.stats.journals.toLocaleString() }}</span>
        </template>
      </v-card>

      <v-card class="bg-white border-gray-200 min-w-60 m-2 grow basis-0" title="OJS Articles" variant="outlined">
        <template #text>
          <p class="text-red-800 text-4xl mb-2 font-semibold mt-0">{{ !data ? '…' : data.stats.articles.toLocaleString() }}</p>
          <p class="mb-0">since 2010</p>
        </template>
      </v-card>

      <v-card class="bg-white border-gray-200 min-w-56 m-2 grow basis-0" title="Hosting Countries" variant="outlined">
        <template #text>
          <p class="text-red-800 text-4xl font-semibold mt-0">{{ !data ? '…' : data.stats.countries }}</p>
        </template>
      </v-card>
    </div>

    <v-card class="bg-white border-gray-200 m-2" title="Number of Yearly OJS Articles" variant="outlined">
      <v-card-text>
        <line-chart
          v-if="data"
          :data="data.yearly_articles"
          data-key="Year"
          data-value="count"
          :height="400"
        />

        <p v-else class="text-red-800 text-4xl font-semibold">…</p>
      </v-card-text>
    </v-card>

    <v-card class="bg-white border-gray-200 m-2 mt-4" title="Countries Hosting OJS Journals" variant="outlined">
      <template #text>
        <country-world-map v-if="data" :data="data.countries" data-key="country_code" data-value="count" />

        <p v-else class="text-red-800 text-4xl font-semibold">…</p>
      </template>
    </v-card>

    <div class="flex flex-wrap justify-between">
      <v-card class="bg-white border-gray-200 min-w-3xs m-2 grow basis-0" title="Topics" variant="outlined">
        <v-card-text>
          <span class="text-xs text-gray-500">The top 10 topics are shown.</span>

          <bar-chart
            v-if="data"
            :data="data.topics.slice(0, 10)"
            data-key="Topic"
            data-value="Count"
            :height="400"
          />

          <p v-else class="text-red-800 text-4xl font-semibold">…</p>
        </v-card-text>
      </v-card>

      <v-card class="bg-white border-gray-200 min-w-3xs m-2 grow basis-0" title="Languages" variant="outlined">
        <v-card-text>
          <span class="text-xs text-gray-500">The top 10 languages are shown.</span>

          <bar-chart
            v-if="data"
            :data="data.languages.slice(0, 10)"
            data-key="Language"
            data-value="Count"
            :height="400"
          />

          <p v-else class="text-red-800 text-4xl font-semibold">…</p>
        </v-card-text>
      </v-card>
    </div>

    <v-card class="bg-white border-gray-200 m-2" variant="outlined">
      <v-card-title>Metadata Quality</v-card-title>

      <v-card-text>
        <v-table v-if="metadataQuality" density="compact">
          <thead>
            <tr>
              <th class="text-left">
                OpenAlex Field
              </th>

              <th class="text-left">
                OJS Articles with Field
              </th>
            </tr>
          </thead>

          <tbody>
            <tr
              v-for="(value, field) in metadataQuality"
              :key="field"
            >
              <td>{{ field }}</td>
              <td>{{ value }}%</td>
            </tr>
          </tbody>
        </v-table>

        <p v-else class="text-red-800 text-4xl font-semibold">…</p>
      </v-card-text>
    </v-card>
  </v-container>
</template>

<script setup>
  import { onMounted, ref } from 'vue'
  import { apiGet } from '@/api'
  import BarChart from '@/components/charts/BarChart.vue'
  import CountryWorldMap from '@/components/charts/CountryWorldMap.vue'
  import LineChart from '@/components/charts/LineChart.vue'

  const data = ref(null)
  const metadataQuality = ref(null)

  onMounted(async () => {
    data.value = await apiGet('/index')
    metadataQuality.value = await apiGet('/metadata_quality')
  })
</script>
