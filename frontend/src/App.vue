<template>
  <v-app>
    <v-app-bar>
      <v-app-bar-nav-icon variant="text" @click.stop="drawer = !drawer" />
      <v-app-bar-title>OJS Dashboard</v-app-bar-title>
    </v-app-bar>

    <v-navigation-drawer
      v-model="drawer"
      :location="$vuetify.display.mobile ? 'bottom' : undefined"
      temporary
    >
      <v-list activatable :items="items" @click:activate="navClicked" />
    </v-navigation-drawer>

    <v-main>
      <router-view />
    </v-main>
  </v-app>
</template>

<script setup>
  import { ref } from 'vue'
  import { useRouter } from 'vue-router'

  const router = useRouter()

  const drawer = ref(false)
  const items = ref([
    { title: 'Start', value: '/' },
    { title: 'Country Analysis', value: '/country' },
    { title: 'Institution Analysis', value: '/institution' },
    { title: 'Journal Analysis', value: '/journal' },
  ])

  function navClicked (item) {
    router.push(item.id)
  }
</script>
