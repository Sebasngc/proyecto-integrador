// =============================================================================
//  Azure Container Apps con GPU serverless (perfil NC A100 / T4).
//  Tercer camino equivalente al de Cloud Run: escala a cero y factura por uso.
//
//  az deployment group create -g <rg> --template-file containerapp.bicep
// =============================================================================
param location string = resourceGroup().location
param environmentName string = 'bigdata-env'
param appName string = 'gpu-preprocess'
param imageRef string

resource env 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: environmentName
  location: location
  properties: {
    workloadProfiles: [
      {
        name: 'gpu-t4'
        workloadProfileType: 'Consumption-GPU-NC8as-T4'  // GPU serverless
      }
    ]
  }
}

resource app 'Microsoft.App/containerApps@2024-03-01' = {
  name: appName
  location: location
  properties: {
    managedEnvironmentId: env.id
    workloadProfileName: 'gpu-t4'
    configuration: {
      ingress: { external: true, targetPort: 8080, transport: 'http' }
    }
    template: {
      containers: [
        {
          name: appName
          image: imageRef
          resources: { cpu: 8, memory: '56Gi' }
          env: [
            { name: 'NORMALIZE_LIB', value: '/opt/lib/libnormalize.so' }
            { name: 'OMP_NUM_THREADS', value: '8' }
          ]
          probes: [
            { type: 'Liveness', httpGet: { path: '/health', port: 8080 }, periodSeconds: 30 }
          ]
        }
      ]
      scale: {
        minReplicas: 0   // escala a cero
        maxReplicas: 4
        rules: [
          {
            name: 'http-rule'
            http: { metadata: { concurrentRequests: '1' } }
          }
        ]
      }
    }
  }
}

output fqdn string = app.properties.configuration.ingress.fqdn
