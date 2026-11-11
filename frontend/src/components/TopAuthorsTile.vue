<template>
  <v-card class="min-w-3xs max-w-2xl flex-1">
    <v-card-title>Top Authors</v-card-title>

    <v-card-text>
      <v-table>
        <thead>
          <tr>
            <th class="text-left">
              Author
            </th>

            <th class="text-left">
              Publications
            </th>
          </tr>
        </thead>

        <tbody>
          <tr
            v-for="item in props.data"
            :key="item.Author"
          >
            <td>{{ item.Author }}</td>
            <td>{{ item.Publications }}</td>
          </tr>
        </tbody>
      </v-table>
    </v-card-text>
  </v-card>
</template>
<script setup>
  const props = defineProps({
    data: {
      type: Array,
      required: true,
    },
  })
</script>
