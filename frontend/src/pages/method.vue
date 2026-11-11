<template>
  <v-container class="mt-10" max-width="900">
    <v-card class="bg-white border-gray-200" variant="outlined">
      <v-card-title>Method</v-card-title>

      <v-card-text>
        The API of OpenAlex is used to query OJS articles with the <span class="font-mono border border-gray-200 bg-white px-1">is_ojs</span> flag.
        Additional information on OJS journals is gathered from the information collected through OJS beacon which is published in the Harvard Dataverse.

        <h3 class="mt-8 mb-2 text-xl">Data Sources</h3>

        <ul class="list-disc ms-4">
          <li><a class="text-red-800" href="https://openalex.org">OpenAlex</a></li>
          <li><a class="text-red-800" href="https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/OCZNVY">OJS Beacon Dataset</a></li>
        </ul>

        <h3 class="mt-8 mb-2 text-xl">Limitations</h3>

        While OpenAlex is the only bibliometric database with an extensive coverage of OJS journals, its data is not without flaws.
        Topic and language assignment is done automatically and can contain errors.
        Author name disambiguation and affiliation are also known erroneous.
        An overview over strength and weaknesses can found in this <a class="text-red-800" href="https://doi.org/10.1016/j.joi.2026.101800">article</a> by Forchino and Torres-Salina.

        <h3 class="mt-8 mb-2 text-xl">Source Code</h3>

        <p>
          The code for the dashboard is available here:
          <a class="text-red-800" href="https://github.com/elya5/ojs-institution-dashboard">github.com/elya5/ojs-institution-dashboard</a>
        </p>
      </v-card-text>
    </v-card>

  </v-container>
</template>
