import { defineStore } from 'pinia'
import { computed, ref } from 'vue'

export const useAppStore = defineStore('app', () => {
  const journals = ref<any[]>([])
  const loading = ref(false)

  const journalCount = computed(() => journals.value.length.toLocaleString())

  async function fetchJournals () {
    if (journals.value.length > 0) {
      return
    }
    loading.value = true
    const res = await fetch('http://localhost:8000/journals')
    journals.value = await res.json()
    loading.value = false
  }

  return { journals, loading, journalCount, fetchJournals }
})
