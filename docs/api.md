# Azul-Client API

For examples on how to use the azul-client Api refer to the examples directory.

The Azul client API is a full mapping of the Azul RestAPI.
The RestAPI is documented with Swagger and viewable through the Azul UI.

Here is the mapping between the azul_client functions and the Swagger documented API endpoints

Table created on 28/05/2025

| METHOD | azul-client function                       | RestAPI URL                                          | description                            |
|--------|--------------------------------------------|------------------------------------------------------|----------------------------------------|
|        | binaries_meta(module)                      | **binaries**                                         |                                        |
| GET    | -                                          | /api/v0/binaries/tags                                | Get All Tags On Entities               |
| POST   | find,find_hashes                           | /api/v0/binaries                                     | Find Binaries                          |
| GET    | -                                          | /api/v0/binaries                                     | Find Binaries                          |
| POST   | find_all                                   | /api/v0/binaries/all                                 | Find All Binaries                      |
| GET    | get_model                                  | /api/v0/binaries/model                               | Get Model                              |
| GET    | find_autocomplete                          | /api/v0/binaries/autocomplete                        | Find Autocomplete                      |
| HEAD   | check_meta                                 | /api/v0/binaries/\{sha256\}                          | Check Metadata Exists                  |
| GET    | get_meta                                   | /api/v0/binaries/\{sha256\}                          | Get Metadata                           |
| GET    | get_has_newer_metadata                     | /api/v0/binaries/\{sha256\}/new                      | Get Has Newer Metadata                 |
| GET    | get_similar_tlsh_entities                  | /api/v0/binaries/similar/tlsh                        | Get Similar Tlsh Entities              |
| GET    | get_similar_ssdeep_entities                | /api/v0/binaries/similar/ssdeep                      | Get Similar Ssdeep Entities            |
| GET    | get_similar_entities                       | /api/v0/binaries/\{sha256\}/similar                  | Get Similar Entities                   |
| GET    | get_nearby_entities                        | /api/v0/binaries/\{sha256\}/nearby                   | Get Nearby Entities                    |
| GET    | get_binary_tags                            | /api/v0/binaries/\{sha256\}/tags                     | Get Entity Tags                        |
| POST   | create_tag_on_binary                       | /api/v0/binaries/\{sha256\}/tags/\{tag\}             | Create Tag On Entity                   |
| DELETE | delete_tag_on_binary                       | /api/v0/binaries/\{sha256\}/tags/\{tag\}             | Delete Tag On Entity                   |
| GET    | get_binary_status                          | /api/v0/binaries/\{sha256\}/statuses                 | Get Entity Status                      |
| GET    | get_binary_documents                       | /api/v0/binaries/\{sha256\}/events                   | Get Entity Documents                   |
|        | binaries_data(module)                      | **binaries_data**                                    |                                        |
| POST   | upload                                     | /api/v0/binaries/source                              | Submit Binary To Source                |
| POST   | upload_dataless                            | /api/v0/binaries/source/dataless                     | Submit Binary To Source Dataless       |
| POST   | upload_child                               | /api/v0/binaries/child                               | Submit Child Binary To Source          |
| POST   | -                                          | /api/v0/binaries/child/dataless                      | Submit Child Binary To Source Dataless |
| POST   | expedite_processing                        | /api/v0/binaries/\{sha256\}/expedite                 | Expedite Processing                    |
| POST   | download_bulk                              | /api/v0/binaries/content/bulk                        | Download Binaries                      |
| HEAD   | check_data                                 | /api/v0/binaries/\{sha256\}/content                  | Check Has Binary                       |
| GET    | download                                   | /api/v0/binaries/\{sha256\}/content                  | Download Binary Encoded                |
| GET    | download_augmented_stream                  | /api/v0/binaries/\{sha256\}/content/\{stream\}       | Download Binary Raw                    |
| GET    | download_hex                               | /api/v0/binaries/\{sha256\}/hexview                  | Get Hex View                           |
| GET    | get_strings                                | /api/v0/binaries/\{sha256\}/strings                  | Get Strings                            |
| GET    | search_hex                                 | /api/v0/binaries/\{sha256\}/search/hex               | Search Hex                             |
|        | features(module)                           | **features**                                         |                                        |
| POST   | count_unique_values_in_feature             | /api/v0/features/values/counts                       | Count Values In Features               |
| POST   | count_unique_entities_in_features          | /api/v0/features/entities/counts                     | Count Entities In Features             |
| POST   | count_unique_entities_in_featurevalues     | /api/v0/features/values/entities/counts              | Count Entities In Featurevalues        |
| POST   | count_unique_entities_in_featurevalueparts | /api/v0/features/values/parts/entities/counts        | Count Entities In Featurevalueparts    |
| GET    | get_all_feature_value_tags                 | /api/v0/features/all/tags                            | Get All Feature Value Tags             |
| GET    | get_feature_values_in_tag                  | /api/v0/features/tags/\{tag\}                        | Get Feature Values In Tag              |
| POST   | create_feature_value_tag                   | /api/v0/features/tags/\{tag\}                        | Create Feature Value Tag               |
| DELETE | delete_feature_value_tag                   | /api/v0/features/tags/\{tag\}                        | Delete Feature Value Tag               |
| GET    | find_features                              | /api/v0/features                                     | Find Features                          |
| GET    | find_values_in_feature                     | /api/v0/features/feature/\{feature\}                 | Find Values In Feature                 |
|        | plugins(module)                            | **plugins**                                          |                                        |
| GET    | get_all_plugins                            | /api/v0/plugins                                      | Get All Plugins                        |
| GET    | get_all_plugin_statuses                    | /api/v0/plugins/status                               | Get All Plugin Statuses                |
| GET    | get_plugin                                 | /api/v0/plugins/\{name\}/versions/\{version\}        | Get Plugin                             |
|        | purge(module)                              | **purge**                                            |                                        |
| DELETE | purge_submission                           | /api/v0/purge/submission/\{track_source_references\} | Purge Submission                       |
| DELETE | purge_link                                 | /api/v0/purge/link/\{track_link\}                    | Purge Link                             |
|        | security(module)                           | **security**                                         |                                        |
| GET    | get_security_settings                      | /api/v0/security                                     | Get Security Settings                  |
| POST   | normalise                                  | /api/v1/security/normalise                           | Normalise Security                     |
| POST   | get_max_security_string                    | /api/v1/security/max                                 | Max Security Strings                   |
| GET    | get_is_user_an_admin                       | /api/v0/security/is_admin                            | Is User Admin Api                      |
|        | sources(module)                            | **sources**                                          |                                        |
| GET    | get_all_sources                            | /api/v0/sources                                      | Get All Sources                        |
| HEAD   | check_source_exists                        | /api/v0/sources/\{source\}                           | Check Source Exists                    |
| GET    | read_source                                | /api/v0/sources/\{name\}                             | Read Source                            |
| GET    | read_source_references                     | /api/v0/sources/\{source\}/references                | Source Refs Read                       |
| GET    | -                                          | /api/v0/sources/\{source\}/submissions               | Source Submissions Read                |
|        | statistics(module)                         | **statistics**                                       |                                        |
| GET    | get_statistics                             | /api/v0/statistics                                   | Get Statistics                         |
|        | users(module)                              | **users**                                            |                                        |
| GET    | get_user_info                              | /api/v0/users/me                                     | Read Users Me                          |
| GET    | get_opensearch_user_info                   | /api/v0/users/me/opensearch                          | Read Users Me                          |
