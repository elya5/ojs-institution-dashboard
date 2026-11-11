<template>
  <v-app>
    <v-container max-width="900">
      <h1 class="mb-4 text-4xl text-red-800">Open Journal System Dashboard</h1>

      <div>
        <v-btn class="me-1" :to="{ name: '/'}" variant="text">
          General
        </v-btn>

        <v-btn class="me-1" :to="{ name: '/institution'}" variant="text">
          Institutions
        </v-btn>

        <v-btn class="me-1" :to="{ name: '/journal'}" variant="text">
          Journals
        </v-btn>

        <v-btn class="me-1" :to="{ name: '/method'}" variant="text">
          Method
        </v-btn>
      </div>
    </v-container>

    <v-main class="bg-gray-50 border-t border-gray-200">
      <router-view />
    </v-main>
  </v-app>
</template>
