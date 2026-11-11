<template>
  <v-card class="min-w-150 max-w-2xl flex-1">
    <v-card-title>Authors' Institutions</v-card-title>

    <v-card-text>
      Data based on Institutions with ROR IDs
      <v-tabs v-model="tab" color="primary" grow>
        <v-tab value="one">Map</v-tab>
        <v-tab value="two">Bar Chart</v-tab>
        <v-tab value="three">Table</v-tab>
      </v-tabs>

      <v-divider />

      <v-tabs-window v-model="tab">
        <v-tabs-window-item value="one">
          <v-sheet class="pa-5" color="purple">
            <scatter-world-map :data="topInstitutions.slice(0, maxInstitutionsMap)" data-key="Institution" data-value="weight" />

            <v-slider
              v-model="maxInstitutionsMap"
              :label="`Show top ${maxInstitutionsMap} institutions`"
              :max="2000"
              :min="10"
              :step="1"
            />
          </v-sheet>
        </v-tabs-window-item>

        <v-tabs-window-item value="two">
          <v-sheet class="pa-5" color="orange">
            <bar-chart
              :data="topInstitutions.slice(0, maxInstitutionsBar)"
              data-key="Institution"
              data-value="weight"
              :height="400"
            />

            <v-slider
              v-model="maxInstitutionsBar"
              :label="`Show top ${maxInstitutionsBar} institutions`"
              :max="50"
              :min="10"
              :step="1"
            />
          </v-sheet>
        </v-tabs-window-item>

        <v-tabs-window-item value="three">
          <v-table>
            <thead>
              <tr>
                <th class="text-left">
                  Institutions
                </th>

                <th class="text-left">
                  Publications
                </th>
              </tr>
            </thead>

            <tbody>
              <tr
                v-for="item in topInstitutions.slice(0, 10)"
                :key="item.Institution"
              >
                <td>{{ item.Institution }}</td>
                <td>{{ item.weight }}</td>
              </tr>
            </tbody>
          </v-table>
        </v-tabs-window-item>
      </v-tabs-window>
    </v-card-text>
  </v-card>
</template>

<script setup>
  import { computed, ref } from 'vue'
  import BarChart from '@/components/charts/BarChart.vue'
  import ScatterWorldMap from '@/components/charts/ScatterWorldMap.vue'

  const tab = ref('one')
  const maxInstitutionsMap = ref(500)
  const maxInstitutionsBar = ref(30)

  const props = defineProps({
    data: {
      type: Array,
      required: true,
    },
  })

  const topInstitutions = computed(() => {
    return props.data.toSorted((a, b) => b.weight - a.weight)
  })
</script>
