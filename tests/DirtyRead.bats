#!/usr/bin/env zunit
# 

@setup {
  # Load a script to set up the test environment
  load tests_scprits.sh
}

@test '(1) DIRTY READ anomaly occured' {
  run dr
  assert "$output" equals "(1,1000)"
}
@test '(2) DIRTY READ anomaly occured' {
  run dr
  assert "$output" equals "(1,1000)"
}
@test '(3) DIRTY READ anomaly occured' {
  run dr
  assert "$output" equals "(1,1000)"
}
@test '(4) DIRTY READ anomaly occured' {
  run dr
  assert "$output" equals "(1,1000)"
}
@test '(5) DIRTY READ anomaly occured' {
  run dr
  assert "$output" equals "(1,1000)"
}
