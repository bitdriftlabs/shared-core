// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use vrl::prelude::Collection;
use vrl::value::Kind;

#[must_use]
pub fn get_report_schema() -> Kind {
  Kind::object(
    Collection::empty()
      .with_known(
        "app_metrics",
        Kind::object(
          Collection::empty()
            .with_known("app_id", Kind::bytes())
            .with_known(
              "build_number",
              Kind::object(
                Collection::empty()
                  .with_known("cf_bundle_version", Kind::bytes())
                  .with_known("version_code", Kind::integer()),
              ),
            )
            .with_known(
              "cpu_usage",
              Kind::object(
                Collection::empty()
                  .with_known("duration_seconds", Kind::integer())
                  .with_known("used_percent", Kind::integer()),
              ),
            )
            .with_known("javascript_engine", Kind::bytes())
            .with_known("lifecycle_event", Kind::bytes())
            .with_known(
              "memory",
              Kind::object(
                Collection::empty()
                  .with_known("free", Kind::integer())
                  .with_known("total", Kind::integer())
                  .with_known("used", Kind::integer()),
              ),
            )
            .with_known("process_id", Kind::integer())
            .with_known("region_format", Kind::bytes())
            .with_known("running_state", Kind::bytes())
            .with_known("version", Kind::bytes()),
        ),
      )
      .with_known(
        "binary_images",
        Kind::array(
          Collection::empty().with_unknown(Kind::object(
            Collection::empty()
              .with_known("id", Kind::bytes())
              .with_known("load_address", Kind::integer())
              .with_known("path", Kind::bytes()),
          )),
        ),
      )
      .with_known(
        "device_metrics",
        Kind::object(
          Collection::empty()
            .with_known("arch", Kind::bytes())
            .with_known(
              "cpu_abis",
              Kind::array(Collection::empty().with_unknown(Kind::bytes())),
            )
            .with_known(
              "cpu_usage",
              Kind::object(
                Collection::empty()
                  .with_known("duration_seconds", Kind::integer())
                  .with_known("used_percent", Kind::integer()),
              ),
            )
            .with_known(
              "display",
              Kind::object(
                Collection::empty()
                  .with_known("density_dpi", Kind::integer())
                  .with_known("height", Kind::integer())
                  .with_known("width", Kind::integer()),
              ),
            )
            .with_known("low_power_mode_enabled", Kind::boolean())
            .with_known("manufacturer", Kind::bytes())
            .with_known("model", Kind::bytes())
            .with_known("network_state", Kind::bytes())
            .with_known(
              "os_build",
              Kind::object(
                Collection::empty()
                  .with_known("brand", Kind::bytes())
                  .with_known("fingerprint", Kind::bytes())
                  .with_known("kern_osversion", Kind::bytes())
                  .with_known("version", Kind::bytes()),
              ),
            )
            .with_known("platform", Kind::bytes())
            .with_known(
              "power_metrics",
              Kind::object(
                Collection::empty()
                  .with_known("charge_percent", Kind::integer())
                  .with_known("power_state", Kind::bytes()),
              ),
            )
            .with_known("rotation", Kind::bytes())
            .with_known("thermal_state", Kind::integer())
            .with_known(
              "time",
              Kind::object(
                Collection::empty()
                  .with_known("nanos", Kind::integer())
                  .with_known("seconds", Kind::integer()),
              ),
            )
            .with_known("timezone", Kind::bytes()),
        ),
      )
      .with_known(
        "errors",
        Kind::array(
          Collection::empty().with_unknown(Kind::object(
            Collection::empty()
              .with_known("name", Kind::bytes())
              .with_known("reason", Kind::bytes())
              .with_known("relation_to_next", Kind::bytes())
              .with_known(
                "stack_trace",
                Kind::array(
                  Collection::empty().with_unknown(Kind::object(
                    Collection::empty()
                      .with_known("class_name", Kind::bytes())
                      .with_known("frame_address", Kind::integer())
                      .with_known("frame_status", Kind::bytes())
                      .with_known("image_id", Kind::bytes())
                      .with_known("in_app", Kind::boolean())
                      .with_known("js_bundle_path", Kind::bytes())
                      .with_known("original_index", Kind::integer())
                      .with_known(
                        "registers",
                        Kind::array(
                          Collection::empty().with_unknown(Kind::object(
                            Collection::empty()
                              .with_known("name", Kind::bytes())
                              .with_known("value", Kind::integer()),
                          )),
                        ),
                      )
                      .with_known(
                        "source_file",
                        Kind::object(
                          Collection::empty()
                            .with_known("column", Kind::integer())
                            .with_known("line", Kind::integer())
                            .with_known("path", Kind::bytes()),
                        ),
                      )
                      .with_known(
                        "state",
                        Kind::array(Collection::empty().with_unknown(Kind::bytes())),
                      )
                      .with_known("symbol_address", Kind::integer())
                      .with_known("symbol_name", Kind::bytes())
                      .with_known("symbolicated_name", Kind::bytes())
                      .with_known("type", Kind::bytes()),
                  )),
                ),
              ),
          )),
        ),
      )
      .with_known(
        "feature_flags",
        Kind::array(
          Collection::empty().with_unknown(Kind::object(
            Collection::empty()
              .with_known("name", Kind::bytes())
              .with_known(
                "timestamp",
                Kind::object(
                  Collection::empty()
                    .with_known("nanos", Kind::integer())
                    .with_known("seconds", Kind::integer()),
                ),
              )
              .with_known("value", Kind::bytes()),
          )),
        ),
      )
      .with_known(
        "sdk",
        Kind::object(
          Collection::empty()
            .with_known("id", Kind::bytes())
            .with_known("version", Kind::bytes()),
        ),
      )
      .with_known(
        "state",
        Kind::array(
          Collection::empty().with_unknown(Kind::object(
            Collection::empty()
              .with_known("key", Kind::bytes())
              .with_known("value", Kind::undefined())
              .with_known("value_type", Kind::bytes()),
          )),
        ),
      )
      .with_known(
        "thread_details",
        Kind::object(
          Collection::empty()
            .with_known("count", Kind::integer())
            .with_known(
              "threads",
              Kind::array(
                Collection::empty().with_unknown(Kind::object(
                  Collection::empty()
                    .with_known("active", Kind::boolean())
                    .with_known("index", Kind::integer())
                    .with_known("name", Kind::bytes())
                    .with_known("priority", Kind::float())
                    .with_known("quality_of_service", Kind::integer())
                    .with_known(
                      "stack_trace",
                      Kind::array(
                        Collection::empty().with_unknown(Kind::object(
                          Collection::empty()
                            .with_known("class_name", Kind::bytes())
                            .with_known("frame_address", Kind::integer())
                            .with_known("frame_status", Kind::bytes())
                            .with_known("image_id", Kind::bytes())
                            .with_known("in_app", Kind::boolean())
                            .with_known("js_bundle_path", Kind::bytes())
                            .with_known("original_index", Kind::integer())
                            .with_known(
                              "registers",
                              Kind::array(
                                Collection::empty().with_unknown(Kind::object(
                                  Collection::empty()
                                    .with_known("name", Kind::bytes())
                                    .with_known("value", Kind::integer()),
                                )),
                              ),
                            )
                            .with_known(
                              "source_file",
                              Kind::object(
                                Collection::empty()
                                  .with_known("column", Kind::integer())
                                  .with_known("line", Kind::integer())
                                  .with_known("path", Kind::bytes()),
                              ),
                            )
                            .with_known(
                              "state",
                              Kind::array(Collection::empty().with_unknown(Kind::bytes())),
                            )
                            .with_known("symbol_address", Kind::integer())
                            .with_known("symbol_name", Kind::bytes())
                            .with_known("symbolicated_name", Kind::bytes())
                            .with_known("type", Kind::bytes()),
                        )),
                      ),
                    )
                    .with_known("state", Kind::bytes())
                    .with_known("summary", Kind::bytes()),
                )),
              ),
            ),
        ),
      )
      .with_known("type", Kind::bytes()),
  )
}
