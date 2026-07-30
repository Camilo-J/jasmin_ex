defmodule JasminEx.StateStore.CompatibilityTest do
  use ExUnit.Case, async: true

  @moduletag :compatibility

  @workflow_path ".github/workflows/ci.yml"
  @integration_command "mix test --include integration test/jasmin_ex/state_store/integration_test.exs"
  @redis_image "redis:8.0.3-bookworm@sha256:be0a135f1955140436b9114da96dd22fbedb874469400b6ef458cc0d42155de0"
  @dragonfly_image "docker.dragonflydb.io/dragonflydb/dragonfly:v1.30.3@sha256:29d44a25a9e6937672f1c12e28c9f481f3d3c0441001ee56ed274a72f50593b7"

  test "defines the pinned Redis 8.0.3 contract as non-blocking evidence" do
    workflow = File.read!(@workflow_path)

    assert_contract(workflow, "state-store-redis-8-0-3", @redis_image)
  end

  test "defines the pinned Dragonfly v1.30.3 contract as non-blocking evidence" do
    workflow = File.read!(@workflow_path)

    assert_contract(workflow, "state-store-dragonfly-v1-30-3", @dragonfly_image)
  end

  test "uploads a result artifact for each non-blocking compatibility contract" do
    workflow = File.read!(@workflow_path)

    assert workflow =~ "actions/upload-artifact@"
    assert workflow =~ "if: ${{ always() }}"
    assert workflow =~ "state-store-compatibility-redis-8-0-3"
    assert workflow =~ "state-store-compatibility-dragonfly-v1-30-3"
  end

  defp assert_contract(workflow, job_name, image) do
    assert workflow =~ job_name
    assert workflow =~ "continue-on-error: true"
    assert workflow =~ "STATE_STORE_TEST_IMAGE: #{image}"
    assert workflow =~ @integration_command
  end
end
